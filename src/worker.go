package spark

// each machine runs only one worker, which can do multiple job at the same time.

type Worker struct { 
  l net.Listener

  name string
  ReduceByName func(*list.List) *list.List   // key , values  -> key, value 
  Reduce func(*list.List) *list.List         // values -> value 
  Map func(*list.List) *list.List            // key, value -> key, value
  
  
  mem map[string] interface{}   // filename -> filedata , this filename is an identifier of any computation result 
  
  nCore // number of cores (thread) can be run on this worker -> initialize this from command arg / config file
  jobThread map[int]
}

// The master sent us a job
func (wk *Worker) DoJob(arg *DoJobArgs, res *DoJobReply) error {
}


// Set up a connection with the master, register with the master,
// and wait for jobs from the master
func RunWorker(MasterAddress string, me string,
               MapFunc func(string) *list.List,
               ReduceFunc func(string,*list.List) string, nRPC int) {
  DPrintf("RunWorker %s\n", me)
  wk := new(Worker)
  
  // Todo: initialize worker
  wk.name = me
  wk.Map = MapFunc
  wk.Reduce = ReduceFunc
  wk.nRPC = nRPC
  
  
  rpcs := rpc.NewServer()
  rpcs.Register(wk)
  os.Remove(me)   // only needed for "unix"
  l, e := net.Listen("unix", me)
  if e != nil {
    log.Fatal("RunWorker: worker ", me, " error: ", e)
  }
  wk.l = l
  Register(MasterAddress, me)

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
  DPrintf("RunWorker %s exit\n", me)    
}
