package spark

import "container/list"
import "net"
//import "net/rpc"
//import "log"
// each machine runs only one worker, which can do multiple job at the same time.

type Worker struct { 
  l net.Listener

  name string // e.g. "127.0.0.1"
  port string // e.g. ":1234"
  ReduceByName func(*list.List) *list.List   // key , values  -> key, value 
  Reduce func(*list.List) *list.List         // values -> value 
  Map func(*list.List) *list.List            // key, value -> key, value
  
  
  mem map[string] interface{}   // filename -> filedata , this filename is an identifier of any computation result 
  
  nCore int // number of cores (thread) can be run on this worker -> initialize this from command arg / config file
  jobThread map[int]int
}
// The master sent us a job
func (wk *Worker) DoJob(arg *DoJobArgs, res *DoJobReply) error {
  // TODO

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
  return nil;
}

// Tell the master we exist and ready to work
func Register(masteraddr string, masterport string, myaddr string, myport string) {
  master := strings.Join([]string{masteraddr, masterport}, "")
  me := strings.Join([]string{myaddr, myport}, "")
  args := &RegisterArgs{}
  args.Worker = me
  var reply RegisterReply
  ok := call(master, "MapReduce.Register", args, &reply)
  if ok == false {
    fmt.Printf("Register: RPC %s register error\n", master)
  }
}

// Set up a connection with the master, register with the master,
// and wait for jobs from the master
func RunWorker(MasterAddress string, MasterPort string, me string, port string,
               MapFunc func(string) *list.List,
               ReduceFunc func(string,*list.List) string, nRPC int) {
  DPrintf("RunWorker %s%s\n", me, port)
  wk := new(Worker)

  // Todo: initialize worker
  wk.name = me
  wk.port = port
  wk.Map = MapFunc
  wk.Reduce = ReduceFunc
  wk.nRPC = nRPC
  rpcs := rpc.NewServer()
  rpcs.Register(wk)
  l, e := net.Listen("tcp", port)
  if e != nil {
    log.Fatal("RunWorker: worker ", me, port, " error: ", e)
  }
  wk.l = l
  Register(MasterAddress, MasterPort, me, port)

  // DON'T MODIFY CODE BELOW
  for wk.nRPC != 0 {
    conn, err := wk.l.Accept()
    if err == nil {
      wk.nRPC -= 1
      go rpcs.ServeConn(conn)
      wk.nJobs += 1
    } else {
      break
    }
  }
  wk.l.Close()
  DPrintf("RunWorker %s%s exit\n", me, port)
}
