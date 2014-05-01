package spark

import "container/list"
import "net"

type WorkerInfo struct {
  address string // e.g. "127.0.0.1:1234"
  nCore int
}

type JobCompleteStatus struct{
  workerId string
  jobId int
  OK bool
}


type Master struct {
  file string  // Name of input file
  MasterAddress string // e.g. "127.0.0.1"
  MasterPort string // e.g. ":1234"
  registerChannel chan string // contains addr:port of workers, e.g. "127.0.0.1:1234"
  DoneChannel chan bool
  alive bool
  l net.Listener
  stats *list.List

  // Map of registered workers that you need to keep up to date
  Workers map[string]*WorkerInfo 

  // add any additional state here
  nAvailableWorkers int  
  availableWorkersList *list.List

  jobCompleteStatus chan JobCompleteStatus
}

func MakeMaster(address string, port string) *Master {
  mr := Master{}
  mr.MasterAddress = address
  mr.MasterPort = port
  mr.alive = true
  mr.registerChannel = make(chan string)
  mr.DoneChannel = make(chan bool)
  mr.StartRegistrationServer()
  go mr.Run()
  return &mr
}

/*
func (mr *Master) runJobThread(workerId string, jobId int, jobType JobType){
}

func (mr *Master) assignJobs(jobType JobType) {

}*/

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *Master) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}


func (mr *Master) Register(args *RegisterArgs, res *RegisterReply) error {
  DPrintf("Register: worker %s\n", args.Worker)
  mr.registerChannel <- args.Worker
  res.OK = true
  return nil
}

func (mr *Master) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
  DPrintf("Shutdown: registration server\n")
  mr.alive = false
  mr.l.Close()    // causes the Accept to fail
  return nil
}

func (mr *Master) StartRegistrationServer() {
  rpcs := rpc.NewServer()
  rpcs.Register(mr)
  l, e := net.Listen("tcp", mr.MasterPort)
  if e != nil {
    log.Fatal("RegstrationServer", mr.MasterAddress, " error: ", e)
  }
  mr.l = l

  // now that we are listening on the master address, can fork off
  // accepting connections to another thread.
  go func() {
    for mr.alive {
      conn, err := mr.l.Accept()
      if err == nil {
        go func() {
          rpcs.ServeConn(conn)
          conn.Close()
        }()
      } else {
        DPrintf("RegistrationServer: accept error", err)
        break
      }
    }
    DPrintf("RegistrationServer: done\n")
  }()
}

func (mr *Master) Run() {
  // TODO split

  // asssign jobs
  mr.stats = mr.RunMaster() // these stats are useful for testing

  // TODO merge

  // TODO cleanup & shutdown

  mr.DoneChannel <- true
}

func (mr *Master) RunMaster() *list.List {
  // TODO assign jobs to workers, exit when finished
  // should call assignJobs()
  // check registerChannel as well as other channels that communicate with workers

  return mr.KillWorkers()
}

