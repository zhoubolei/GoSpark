package spark

import (
  "container/list"
  "net"
  "net/rpc"
  "hadoop"
  "strings"
  "fmt"
  "log"
  "sync"
  "reflect"
)
// each machine runs only one worker, which can do multiple job at the same time.

type Worker struct { 
  l net.Listener
  nRPC int
  nJobs int
  alive bool

  name string // e.g. "127.0.0.1"
  port string // e.g. ":1234"
  
  mu sync.RWMutex
  mem map[string]([]interface{})   // filename -> line id -> filedata , this filename is an identifier of any computation result 
  
  nCore int // number of cores (thread) can be run on this worker -> initialize this from command arg / config file
  jobThread map[int]int
}

func (wk *Worker) reduce_list(p *list.Element, data interface{}, fn reflect.Value) interface{} {
  if p.Next() == nil {
    return p.Value
  } else {
    // apply reducer function recursively
    head := p.Value
    tail := wk.reduce_list(p.Next(), data, fn)
    // call function by name
    // convert to KeyValue before calling UserFunc
    a1 := reflect.ValueOf(KeyValue{Value:head})
    a2 := reflect.ValueOf(KeyValue{Value:tail})
    a3 := reflect.ValueOf(KeyValue{Value:data})
    r := fn.Call([]reflect.Value{a1, a2, a3}) // TODO check function type
    return r[0].Interface()
  }
}

func (wk *Worker) read_split(hostname string, splitID string) ([]interface{}, bool) {
  my_name := strings.Join([]string{wk.name, wk.port}, "")
  if hostname == "" || hostname == my_name { // read from local mem
    wk.mu.RLock()
    defer wk.mu.RUnlock()
    arr, exist := wk.mem[splitID]
    return arr, exist
  } else { // ask another worker
    args := DoJobArgs{Operation:GetSplit, InputID:splitID}
    var reply DoJobReply
    ok := call(hostname, "Worker.DoJob", args, &reply)
    if ok == false || reply.OK == false {
      DPrintf("fetch failed: worker %s, split %s", hostname, splitID)
      return nil, false
    }
    // store to local mem
    wk.mu.Lock()
    wk.mem[splitID] = reply.Result.([]interface{})
    wk.mu.Unlock()
    return reply.Result.([]interface{}), true
  }
}

// return (split content, exists or not, the split id used)
func (wk *Worker) read_one_input(id string, ids []Split) ([]interface{}, bool, string) {
  if id == "" {
    if ids == nil || len(ids) < 1 {
      DPrintf("no split to read")
      return nil, false, ""
    }
    s, exist := wk.read_split(ids[0].Hostname, ids[0].SplitID)
    return s, exist, ids[0].SplitID
  } else {
    s, exist := wk.read_split("", id)
    return s, exist, id
  }
}

// return (merged split content, all exist or not, the splits missing)
func (wk *Worker) read_mult_inputs(ids []Split) ([]interface{}, bool, []string) {
  everything := list.New()
  missing := list.New()
  success := true
  // try reading all the splits
  for _, id := range ids {
    arr, exist := wk.read_split(id.Hostname, id.SplitID)
    if !exist {
      success = false
      missing.PushBack(id.SplitID)
    }
    if !success {
      continue
    }
    for _, line := range arr {
      everything.PushBack(line)
    }
  }
  if !success { // some splits missing, return their ids
    // conver to array
    n := missing.Len()
    p := missing.Front()
    arr := make([]string, n)
    for i := 0; i < n; i++ {
      arr[i] = p.Value.(string)
      p = p.Next()
    }
    return nil, false, arr
  } else { // all splits exist, return merged contents
    // conver to array
    n := everything.Len()
    p := everything.Front()
    arr := make([]interface{}, n)
    for i := 0; i < n; i++ {
      arr[i] = p.Value
      p = p.Next()
    }
    return arr, true, nil
  }
}

// The master sent us a job
func (wk *Worker) DoJob(args *DoJobArgs, res *DoJobReply) error {
  DPrintf("worker %s%s DoJob %v", wk.name, wk.port, args)

  res.Result = nil
  res.OK = false
  res.NeedSplits = nil

  switch args.Operation {
  case ReadHDFSSplit:
    // read whole split from HDFS
    lines := list.New()
    scanner, err := hadoop.GetSplitScanner(args.HDFSFile, args.HDFSSplitID) // get the scanner of current split
    if err != nil {
      DPrintf("error reading file")
      res.OK = false
      return nil
    }
    for scanner.Scan() {
      lines.PushBack(scanner.Text()) // read one line of data in the split
    }
    // convert to array
    n := lines.Len()
    p := lines.Front()
    arr := make([]interface{}, n)
    for i := 0; i < n; i++ {
      arr[i] = p.Value
      p = p.Next()
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = arr
    wk.mu.Unlock()
    // reply
    res.Result = len(arr) // line count
    res.OK = true

  case HasSplit:
    _, res.Result, _ = wk.read_one_input(args.InputID, args.InputIDs)
    res.OK = true

  case GetSplit:
    arr, exists, id := wk.read_one_input(args.InputID, args.InputIDs)
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{id}
      return nil
    }
    DPrintf("GetSplit read %v %v", args.InputID, arr)
    res.Result = arr // split content XXX no
    //res.Result = []UserData{UserData{Data:"data"}} // no
    //res.Data = []UserData{UserData{Data:"data"}} // yes
    //res.Data = []UserData{UserData{Data:KeyValue{Key:UserData{Data:"key"}, Value:UserData{Data:"value"}}}} // no
    //res.Result = []KeyValue{KeyValue{Key:UserData{Data:"key"}, Value:UserData{Data:"value"}}} // no
    //res.Result = KeyValue{Key:UserData{Data:"key"}, Value:UserData{Data:"value"}} // no
    res.OK = true

  case Count: // TODO remove?
    arr, exists, id := wk.read_one_input(args.InputID, args.InputIDs)
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{id}
      return nil
    }
    res.Result = len(arr) // line count
    res.OK = true

  case MapJob:
    arr, exists, id := wk.read_one_input(args.InputID, args.InputIDs)
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{id}
      return nil
    }
    // perform mapper function on each line
    out := make([]interface{}, len(arr))
    for i, line := range arr {
      // look up function by name
      fn := reflect.ValueOf(&UserFunc{}).MethodByName(args.Function)
      if !fn.IsValid() {
        DPrintf("undefined")
        res.OK = false
        return nil
      }
      // call function by name
      // convert to KeyValue before calling UserFunc
      a1 := reflect.ValueOf(KeyValue{Value:line})
      a2 := reflect.ValueOf(KeyValue{Value:args.Data})
      r := fn.Call([]reflect.Value{a1, a2}) // TODO check function type
      out[i] = r[0].Interface()
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = out
    wk.mu.Unlock()
    // reply
    DPrintf("Map out %v", out)
    res.OK = true

  case ReduceByKeyJob:
    kv := make(map[interface{}]*list.List) // key -> list of values
    lines, complete, missing := wk.read_mult_inputs(args.InputIDs)
    // some splits missing
    if !complete {
      DPrintf("not found %v", missing)
      // reply
      res.OK = false
      res.NeedSplits = missing
      return nil
    }
    // all splits present
    // sort by key
    for _, line := range lines {
      k := line.(KeyValue).Key
      v := line.(KeyValue).Value
      _, allocated := kv[k]
      if !allocated {
        kv[k] = list.New()
      }
      kv[k].PushBack(v)
    }
    // perform reducer function
    for k, vl := range kv {
      if vl == nil || vl.Len() == 0 {
        delete(kv, k)
      }
    }
    DPrintf("Shuffled %v", kv)
    out := make([]interface{}, len(kv)) // each line for one key
    i := 0
    for k, vl := range kv {
      // look up function by name
      fn := reflect.ValueOf(&UserFunc{}).MethodByName(args.Function)
      if !fn.IsValid() {
        DPrintf("undefined")
        res.OK = false
        return nil
      }
      // perform reducer function on the list of values
      out[i] = KeyValue{Key:k, Value:wk.reduce_list(vl.Front(), args.Data, fn)}
      i++
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = out
    wk.mu.Unlock()
    // reply
    DPrintf("Reduce out %v %v", args.OutputID, out)
    res.OK = true

  default:
    DPrintf("unknown job")
    res.OK = false

  }

  DPrintf("success reply %v", res)

  return nil
}

// The master is telling us to shutdown. Report the number of Jobs we
// have processed.
func (wk *Worker) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
  DPrintf("Shutdown %s\n", wk.name)
  res.Njobs = wk.nJobs
  res.OK = true
  wk.nRPC = 1   // OK, because the same thread reads nRPC
  wk.nJobs--   // Don't count the shutdown RPC
  wk.alive = false
  wk.l.Close()    // causes the Accept to fail
  return nil;
}

// Tell the master we exist and ready to work
func Register(masteraddr string, masterport string, myaddr string, myport string) {
  master := strings.Join([]string{masteraddr, masterport}, "")
  me := strings.Join([]string{myaddr, myport}, "")
  args := &RegisterArgs{}
  args.Worker = me
  var reply RegisterReply
  ok := call(master, "Master.Register", args, &reply)
  if ok == false {
    fmt.Printf("Register: RPC %s register error\n", master)
  }
}

// Set up a connection with the master, register with the master,
// and wait for jobs from the master
func RunWorker(MasterAddress string, MasterPort string, me string, port string, nRPC int) {
  DPrintf("RunWorker %s%s\n", me, port)
  wk := new(Worker)

  wk.name = me
  wk.port = port
  wk.nRPC = nRPC
  wk.alive = true
  rpcs := rpc.NewServer()
  rpcs.Register(wk)
  l, e := net.Listen("tcp", port)
  if e != nil {
    log.Fatal("RunWorker: worker ", me, port, " error: ", e)
  }
  wk.l = l
  wk.mem = make(map[string]([]interface{}))
  Register(MasterAddress, MasterPort, me, port)
  // TODO if idle for some time, register again

  // DON'T MODIFY CODE BELOW
  for wk.nRPC != 0 && wk.alive {
    conn, err := wk.l.Accept()
    if err == nil {
      wk.nRPC -= 1
      go rpcs.ServeConn(conn)
      wk.nJobs += 1
    } else {
      break
    }
  }
  DPrintf("RunWorker %s%s exit\n", me, port)
}
