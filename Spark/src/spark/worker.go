package spark

import (
  "net"
  "net/rpc"
  "hadoop"
  "strings"
  "fmt"
  "log"
  "sync"
  "reflect"
  "time"
  "math/rand"
  "encoding/gob"
  "syscall"
  "runtime"
)

const (
  PingInterval          = 100 * time.Millisecond
  PingFailInterval      = time.Second
  WaitLockInterval      = 10 * time.Millisecond
  WorkloadInterval      = 5 * time.Second
)


// each machine runs only one worker, which can do multiple job at the same time.

type Worker struct { 
  l net.Listener
  nJobs int
  lastRPC time.Time
  mu_run sync.RWMutex
  running int
  alive bool
  unreliable bool // randomly discard RPC requests or replies. for testing.
  DoneChannel chan bool

  name string // e.g. "127.0.0.1"
  port string // e.g. ":1234"
  
  mu sync.RWMutex
  mem map[string]([]interface{})   // filename -> line id -> filedata , this filename is an identifier of any computation result 
  
  nCore int // number of cores (thread) can be run on this worker -> initialize this from command arg / config file
  jobThread map[int]int
}

func (wk *Worker) reduce_slice(arr []interface{}, data interface{}, fn reflect.Value) interface{} {
  if len(arr) == 0 {
    return nil
  } else if len(arr) == 1 {
    return arr[0]
  } else {
    s := arr[0]
    var r []reflect.Value
    for i := 1; i < len(arr); i++ {
      // call function by name
      // convert to KeyValue before calling UserFunc
      // because reflect doesn't support interface{} as arguments
      a1 := reflect.ValueOf(KeyValue{Value:s})
      a2 := reflect.ValueOf(KeyValue{Value:arr[i]})
      if data != nil {
        a3 := reflect.ValueOf(KeyValue{Value:data})
        r = fn.Call([]reflect.Value{a1, a2, a3}) // TODO check function type
      } else {
        r = fn.Call([]reflect.Value{a1, a2}) // TODO check function type
      }
      s = r[0].Interface()
    }
    return s
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
    wk.mem[splitID] = reply.Lines
    wk.mu.Unlock()
    return reply.Lines, true
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
  everything := make([]interface{}, 0)
  missing := make([]string, 0)
  success := true
  // try reading all the splits
  for _, id := range ids {
    arr, exist := wk.read_split(id.Hostname, id.SplitID)
    if !exist {
      success = false
      missing = append(missing, id.SplitID)
    }
    if !success {
      continue
    }
    for _, line := range arr {
      everything = append(everything, line)
    }
  }
  if !success { // some splits missing, return their ids
    return nil, false, missing
  } else { // all splits exist, return merged contents
    return everything, true, nil
  }
}

func (wk *Worker) convert_table(tbl []interface{}) map[interface{}]interface{} { // assume keys are unique
  kv := make(map[interface{}]interface{})
  for _, t := range tbl {
    k := t.(KeyValue).Key
    v := t.(KeyValue).Value
    kv[k] = v
  }
  return kv
}

func (wk *Worker) outputs_already_exist(id string, ids []Split) bool {
  if id != "" {
    _, exist := wk.read_split("", id)
    if !exist {
      return false
    }
  }
  if ids != nil {
    for _, s := range ids {
      _, exist := wk.read_split("", s.SplitID)
      if !exist {
        return false
      }
    }
  }
  return true
}

func (wk *Worker) cnt_inc() {
  wk.mu_run.Lock()
  wk.running++
  wk.mu_run.Unlock()
}

func (wk *Worker) cnt_dec() {
  wk.mu_run.Lock()
  wk.running--
  wk.mu_run.Unlock()
}

func (wk *Worker) cnt_get() int {
  wk.mu_run.RLock()
  defer wk.mu_run.RUnlock()
  return wk.running
}

func (wk *Worker) cnt_wait() {
  for wk.alive && wk.cnt_get() > 0 {
    time.Sleep(WaitLockInterval)
  }
}

// The master sent us a job
func (wk *Worker) DoJob(args *DoJobArgs, res *DoJobReply) error {
  DPrintf("worker %s%s DoJob %v", wk.name, wk.port, args)

  start := time.Now()
  wk.cnt_inc()
  defer wk.cnt_dec()

  res.Result = nil
  res.OK = false
  res.NeedSplits = nil

  if args.Operation == ReadHDFSSplit {

    // ignore duplicates
    if wk.outputs_already_exist(args.OutputID, nil) {
      DPrintf("output already exists")
      res.OK = true
      res.Latency = time.Since(start)
      return nil
    }
    // read whole split from HDFS
    lines := make([]interface{}, 0)
    scanner, err := hadoop.GetSplitScanner(args.HDFSFile, args.HDFSSplitID) // get the scanner of current split
    if err != nil {
      DPrintf("error reading file")
      res.OK = false
      res.Latency = time.Since(start)
      return nil
    }
    for scanner.Scan() {
      lines = append(lines, scanner.Text()) // read one line of data in the split
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = lines
    wk.mu.Unlock()
    // reply
    res.Result = len(lines) // line count
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == HasSplit {

    _, res.Result, _ = wk.read_one_input(args.InputID, args.InputIDs)
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == GetSplit {

    // prepare the split
    arr, exists, id := wk.read_one_input(args.InputID, args.InputIDs)
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{id}
      res.Latency = time.Since(start)
      return nil
    }
    DPrintf("GetSplit read %v %v", args.InputID, arr)
    res.Lines = arr // split content
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == DelSplit {

    // delete from memory
    wk.mu.Lock()
    delete(wk.mem, args.InputID)
    wk.mu.Unlock()
    // reply
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == Count {

    arr, exists, id := wk.read_one_input(args.InputID, args.InputIDs)
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{id}
      res.Latency = time.Since(start)
      return nil
    }
    res.Result = len(arr) // line count
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == MapJob || args.Operation == FlatMapJob || args.Operation == MapValuesJob || args.Operation == FilterJob {

    // ignore duplicates
    if wk.outputs_already_exist(args.OutputID, nil) {
      DPrintf("output already exists")
      res.OK = true
      res.Latency = time.Since(start)
      return nil
    }
    // look up function by name
    fn := reflect.ValueOf(&UserFunc{}).MethodByName(args.Function)
    if !fn.IsValid() {
      DPrintf("undefined")
      res.OK = false
      res.Latency = time.Since(start)
      return nil
    }
    // prepare inputs
    arr, exists, id := wk.read_one_input(args.InputID, args.InputIDs)
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{id}
      res.Latency = time.Since(start)
      return nil
    }
    // perform mapper function on each line
    out := make([]interface{}, 0)
    var r []reflect.Value
    for _, line := range arr {
      if args.Operation == MapJob || args.Operation == FlatMapJob {
        // call function by name
        // convert to KeyValue before calling UserFunc
        // because reflect doesn't support interface{} as arguments
        a1 := reflect.ValueOf(KeyValue{Value:line})
        if args.Data != nil {
          a2 := reflect.ValueOf(KeyValue{Value:args.Data})
          r = fn.Call([]reflect.Value{a1, a2}) // TODO check function type
        } else {
          r = fn.Call([]reflect.Value{a1}) // TODO check function type
        }
        out = append(out, r[0].Interface())
      } else if args.Operation == MapValuesJob {
        k := line.(KeyValue).Key
        v := line.(KeyValue).Value
        a1 := reflect.ValueOf(KeyValue{Value:v})
        if args.Data != nil {
          a2 := reflect.ValueOf(KeyValue{Value:args.Data})
          r = fn.Call([]reflect.Value{a1, a2}) // TODO check function type
        } else {
          r = fn.Call([]reflect.Value{a1}) // TODO check function type
        }
        out = append(out, KeyValue{Key:k, Value:r[0].Interface()}) // <old key, new value>
      } else { // FilterJob
        a1 := reflect.ValueOf(KeyValue{Value:line})
        if args.Data != nil {
          a2 := reflect.ValueOf(KeyValue{Value:args.Data})
          r = fn.Call([]reflect.Value{a1, a2}) // TODO check function type
        } else {
          r = fn.Call([]reflect.Value{a1}) // TODO check function type
        }
        if r[0].Interface().(bool) {
          out = append(out, line)
        }
      }
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = out
    wk.mu.Unlock()
    // reply
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == SampleJob {

    // ignore duplicates
    if wk.outputs_already_exist(args.OutputID, nil) {
      DPrintf("output already exists")
      res.OK = true
      res.Latency = time.Since(start)
      return nil
    }
    // prepare inputs
    arr, exists, id := wk.read_one_input(args.InputID, args.InputIDs)
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{id}
      res.Latency = time.Since(start)
      return nil
    }
    // perform random sample
    total := len(arr)
    r := rand.New(rand.NewSource(args.SampleSeed))
    out := make([]interface{}, 0)
    for i := 0; i < args.SampleN; i++ {
      j := r.Intn(total)
      out = append(out, arr[j])
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = out
    wk.mu.Unlock()
    // reply
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == HashPartJob {

    // ignore duplicates
    if wk.outputs_already_exist(args.OutputID, args.OutputIDs) {
      DPrintf("outputs already exist")
      res.OK = true
      res.Latency = time.Since(start)
      return nil
    }
    // prepare inputs
    arr, exists, id := wk.read_one_input(args.InputID, args.InputIDs)
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{id}
      res.Latency = time.Since(start)
      return nil
    }
    // perform hash partition on each line
    n := len(args.OutputIDs)
    out := make([]([]interface{}), n)
    for i := 0; i < n; i++ {
      out[i] = make([]interface{}, 0)
    }
    for _, line := range arr {
      k := line.(KeyValue).Key
      p := hash(k) % int64(n)
      out[p] = append(out[p], line)
    }
    // store to memory
    wk.mu.Lock()
    for i := 0; i < n; i++ {
      //DPrintf("Hash partition %d %v", i, out[i])
      wk.mem[args.OutputIDs[i].SplitID] = out[i]
    }
    wk.mu.Unlock()
    // reply
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == ReduceJob {

    // look up function by name
    fn := reflect.ValueOf(&UserFunc{}).MethodByName(args.Function)
    if !fn.IsValid() {
      DPrintf("undefined")
      res.OK = false
      res.Latency = time.Since(start)
      return nil
    }
    // prepare inputs
    lines, complete, missing := wk.read_mult_inputs(args.InputIDs)
    // some splits missing
    if !complete {
      DPrintf("not found %v", missing)
      // reply
      res.OK = false
      res.NeedSplits = missing
      res.Latency = time.Since(start)
      return nil
    }
    // all splits present
    // perform reducer function
    res.Result = wk.reduce_slice(lines, args.Data, fn) // reduce to one result
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == ReduceByKeyJob {

    // ignore duplicates
    if wk.outputs_already_exist(args.OutputID, nil) {
      DPrintf("output already exists")
      res.OK = true
      res.Latency = time.Since(start)
      return nil
    }
    // look up function by name
    fn := reflect.ValueOf(&UserFunc{}).MethodByName(args.Function)
    if !fn.IsValid() {
      DPrintf("undefined")
      res.OK = false
      res.Latency = time.Since(start)
      return nil
    }
    // prepare inputs
    lines, complete, missing := wk.read_mult_inputs(args.InputIDs)
    // some splits missing
    if !complete {
      DPrintf("not found %v", missing)
      // reply
      res.OK = false
      res.NeedSplits = missing
      res.Latency = time.Since(start)
      return nil
    }
    // all splits present
    // sort by key
    kv := make(map[interface{}]([]interface{})) // key -> slice of values
    for _, line := range lines {
      k := line.(KeyValue).Key
      v := line.(KeyValue).Value
      _, allocated := kv[k]
      if !allocated {
        kv[k] = make([]interface{}, 0)
      }
      kv[k] = append(kv[k], v)
    }
    // perform reducer function
    out := make([]interface{}, len(kv)) // each line for one key
    i := 0
    for k, vl := range kv {
      // perform reducer function on the list of values
      out[i] = KeyValue{Key:k, Value:wk.reduce_slice(vl, args.Data, fn)} // <original key, reduced value>
      i++
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = out
    wk.mu.Unlock()
    // reply
    res.OK = true
    res.Latency = time.Since(start)

  } else if args.Operation == JoinJob {

    // ignore duplicates
    if wk.outputs_already_exist(args.OutputID, nil) {
      DPrintf("output already exists")
      res.OK = true
      res.Latency = time.Since(start)
      return nil
    }
    // prepare inputs
    tbl1, c1, m1 := wk.read_mult_inputs(args.InputIDs)
    tbl2, c2, m2 := wk.read_mult_inputs(args.InputIDs2)
    // some splits missing
    if !c1 || !c2 {
      DPrintf("not found %v %v", m1, m2)
      // reply
      res.OK = false
      res.NeedSplits = append(m1, m2...)
      res.Latency = time.Since(start)
      return nil
    }
    // all splits present
    // perform inner join
    kv1 := wk.convert_table(tbl1)
    kv2 := wk.convert_table(tbl2)
    inner_join := make([]interface{}, 0)
    for k, v1 := range kv1 {
      v2, exist := kv2[k]
      if exist {
        inner_join = append(inner_join, KeyValue{Key:k, Value:Pair{First:v1, Second:v2}})
      }
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = inner_join
    wk.mu.Unlock()
    // reply
    res.OK = true
    res.Latency = time.Since(start)

  } else {

    DPrintf("unknown job: %s", args.Operation)
    res.OK = false
    res.Latency = time.Since(start)
    return nil

  }

  return nil
}

// The master is telling us to shutdown. Report the number of Jobs we
// have processed.
func (wk *Worker) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
  DPrintf("Shutdown %s\n", wk.name)
  res.Njobs = wk.nJobs
  res.OK = true
  wk.nJobs--   // Don't count the shutdown RPC
  wk.alive = false
  wk.l.Close()    // causes the Accept to fail
  return nil;
}

// Tell the master we exist and ready to work
func (wk *Worker) register(masteraddr string, masterport string) bool {
  args := &RegisterArgs{}
  args.Worker = strings.Join([]string{wk.name, wk.port}, "")
  args.NCore = runtime.NumCPU() // number of logical CPUs on the local machine
  args.Running = wk.cnt_get() // number of jobs currently running

  runtime.GC() // run garbage collection
  var m runtime.MemStats
  runtime.ReadMemStats(&m)
  args.MemUse = m.Alloc // current memory usage

  master := strings.Join([]string{masteraddr, masterport}, "")
  var reply RegisterReply
  ok := call(master, "Master.Register", args, &reply)
  if ok == false {
    fmt.Printf("Register: RPC %s register error\n", master)
    return false
  }
  return true
}

func (wk *Worker) start_wait_for_jobs() {
  rpcs := rpc.NewServer()
  rpcs.Register(wk)
  l, e := net.Listen("tcp", wk.port)
  if e != nil {
    log.Fatal("RunWorker: worker ", wk.name, wk.port, " error: ", e)
  }
  wk.l = l

  // now that we are listening on the worker address, can fork off
  // accepting connections to another thread.
  go func() {
    for wk.alive {
      conn, err := wk.l.Accept()
      if err == nil && wk.alive {
        if wk.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          DPrintf("worker rpc: discard request")
          conn.Close()
        } else if wk.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          DPrintf("worker rpc: process request but discard reply")
          c1 := conn.(*net.TCPConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          wk.lastRPC = time.Now()
          go rpcs.ServeConn(conn)
          wk.nJobs += 1
        } else {
          // normal
          wk.lastRPC = time.Now()
          go rpcs.ServeConn(conn)
          wk.nJobs += 1
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && wk.alive {
        fmt.Printf("worker %s%s accept: %v\n", wk.name, wk.port, err.Error())
        break
      }
    }
    DPrintf("RunWorker %s%s exit\n", wk.name, wk.port)
    wk.DoneChannel <- true
  }()

  DPrintf("worker: ready")
}

// Set up a connection with the master, register with the master,
// and wait for jobs from the master
func MakeWorker(MasterAddress string, MasterPort string, me string, port string, unrel bool) *Worker {
  gob.Register(KeyValue{})
  gob.Register(Pair{})
  register_types() // register custom types

  DPrintf("RunWorker %s%s\n", me, port)
  wk := Worker{}

  wk.name = me
  wk.port = port
  wk.alive = true
  wk.unreliable = unrel
  wk.DoneChannel = make(chan bool)
  wk.mem = make(map[string]([]interface{}))
  wk.lastRPC = time.Now()

  // listen to RPC calls
  wk.start_wait_for_jobs()

  // send ping to master each PingInterval, with current job count and memory use
  go func() {
    for wk.alive {
      ok := wk.register(MasterAddress, MasterPort)
      time.Sleep(PingInterval)
      if !ok {
        time.Sleep(PingFailInterval)
      }
    }
  }()

  // tick, print number of jobs running
  go func() {
    for wk.alive {
      time.Sleep(WorkloadInterval)
      load := wk.cnt_get()
      for i := 0; i < load; i++ {
        fmt.Printf(".")
      }
      fmt.Printf("\n")
    }
  }()

  return &wk
}

// tell the worker to shut itself down. for testing.
func (wk *Worker) kill() {
  DPrintf("worker killed")
  wk.alive = false
  if wk.l != nil {
    wk.l.Close()
  }
}
