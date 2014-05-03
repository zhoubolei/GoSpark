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

// The master sent us a job
func (wk *Worker) DoJob(args *DoJobArgs, res *DoJobReply) error {
  DPrintf("worker %s%s DoJob %v", wk.name, wk.port, args)

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
      arr[i] = p.Value.(string)
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
    wk.mu.RLock()
    _, exists := wk.mem[args.InputID]
    wk.mu.RUnlock()
    res.Result = exists
    res.OK = true

  case GetSplit:
    wk.mu.RLock()
    arr, exists := wk.mem[args.InputID]
    wk.mu.RUnlock()
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{args.InputID}
      return nil
    }
    res.Result = arr // split content
    res.OK = true

  case Count:
    wk.mu.RLock()
    arr, exists := wk.mem[args.InputID]
    wk.mu.RUnlock()
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{args.InputID}
      return nil
    }
    res.Result = len(arr) // line count
    res.OK = true

  case MapJob:
    wk.mu.RLock()
    arr, exists := wk.mem[args.InputID]
    wk.mu.RUnlock()
    if !exists {
      DPrintf("not found")
      res.OK = false
      res.NeedSplits = []string{args.InputID}
      return nil
    }
    // perform mapper function on each line
    out := make([]interface{}, len(arr))
    for i, line := range arr {
      // call function by name
      val := reflect.ValueOf(line)
      fn := val.MethodByName(args.Function)
      if !fn.IsValid() {
        DPrintf("undefined")
        res.OK = false
        return nil
      }
      out[i] = fn.Call([]reflect.Value{val})[0].Interface()
    }
    // store to memory
    wk.mu.Lock()
    wk.mem[args.OutputID] = out
    wk.mu.Unlock()
    // reply
    res.OK = true

  default:
    DPrintf("unknown job")
    res.OK = false

  }

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
