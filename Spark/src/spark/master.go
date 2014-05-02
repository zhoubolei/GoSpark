package spark

import (
  "container/list"
  "net"
  "net/rpc"
  "hadoop"
  "log"
)

const Debug=1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type WorkerInfo struct {
  address string // addr:port of the worker, e.g. "127.0.0.1:1234"
  nCore int      // TODO implement worker threads
  splitId int    // TODO peterkty: this should be specified in DoJobArgs 
}

type Master struct {
  // TODO peterkty: these should be specified in DoJobArgs 
  file string  // Name of input file
  nsplits int
  operation JobType
  // End TODO
  MasterAddress string // e.g. "127.0.0.1"
  MasterPort string // e.g. ":1234"
  registerChannel chan RegisterArgs
  DoneChannel chan bool
  alive bool
  l net.Listener
  stats *list.List

  // Map of registered workers that you need to keep up to date
  Workers map[string]*WorkerInfo 

  // add any additional state here
  workerSuccChannel chan string
  workerFailChannel chan string
  jobsStatus map[int]int // splitId -> 0:unassigned, 1:working, 2:finished
}

func MakeMaster(file string, operation JobType, master string, port string) *Master {
  mr := Master{}
  mr.file = file
  mr.nsplits = hadoop.GetSplitInfo(mr.file).Len()
  DPrintf("this file has %d splits", mr.nsplits)
  mr.operation = operation
  mr.MasterAddress = master
  mr.MasterPort = port
  mr.alive = true
  mr.registerChannel = make(chan RegisterArgs)
  mr.DoneChannel = make(chan bool)
  mr.StartRegistrationServer()
  go mr.Run()
  return &mr
}

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
      DPrintf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}


func (mr *Master) Register(args *RegisterArgs, res *RegisterReply) error {
  DPrintf("Register: worker %s\n", args.Worker)
  mr.registerChannel <- *args
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

func (mr *Master) assignJob(w string, splitid int) {
  args := DoJobArgs{Operation:mr.operation, File:mr.file, SplitID:splitid}
  var reply DoJobReply
  ok := call(w, "Worker.DoJob", args, &reply)
  if ok == false { // RPC fails, keep looking for workers for the current job
    DPrintf("assignJob RPC failed")
    mr.workerFailChannel <- w
  } else if reply.OK == false { // somehow fails the job, also needs reassignment
    DPrintf("assignJob reply failed")
    mr.workerFailChannel <- w
  } else { // current job is done, fetch result, ready for the next job
    if mr.fetchResult(w, splitid) {
      mr.workerSuccChannel <- w
    } else {
      mr.workerFailChannel <- w // although compute success, fail to fetch result
    }
  }
}

func (mr *Master) fetchResult(w string, splitid int) bool {
  args := FetchArgs{File:mr.file, SplitID:splitid}
  var reply FetchReply
  ok := call(w, "Worker.Fetch", args, &reply)
  if ok == false {
    DPrintf("fetchResult RPC failed")
    return false
  } else if reply.OK == false {
    DPrintf("fetchResult reply failed")
    return false
  } else {
    DPrintf("worker %s split %d result %v", w, splitid, reply.Result)
    return true
  }
}

func (mr *Master) RunMaster() *list.List {

  mr.Workers = make(map[string]*WorkerInfo)
  mr.workerSuccChannel = make(chan string)
  mr.workerFailChannel = make(chan string)
  mr.jobsStatus = make(map[int]int)
  for j := 0; j < mr.nsplits; j++ {
    mr.jobsStatus[j] = 0
  }

  // assign jobs to workers, exit when finished
  for {
    // listen to workers
    var w string
    var wk RegisterArgs
    var ok bool
    avail := false
    select {
    case wk, ok = <-mr.registerChannel:
      if ok {
        w = wk.Worker
        mr.Workers[w] = &WorkerInfo{address:w, nCore:wk.NCore, splitId:-1}
        avail = true
        //DPrintf("received registration from worker %s", w)
      } else {
        DPrintf("register channel closed")
      }
    case w, ok = <-mr.workerSuccChannel:
      if ok {
        mr.jobsStatus[mr.Workers[w].splitId] = 2 // mark job finished
        avail = true // this worker is good
        //DPrintf("received success from worker %s", w)
      } else {
        DPrintf("worker success channel closed")
      }
    case w, ok = <-mr.workerFailChannel:
      if ok {
        mr.jobsStatus[mr.Workers[w].splitId] = 0 // reverse to unassigned
        avail = false // this worker is bad
        //DPrintf("received failure from worker %s", w)
      } else {
        DPrintf("worker failure channel closed")
      }
    }
    if !avail {
      continue
    }

    // worker available, assign a new job if remaining
    assigned := false
    for j := range mr.jobsStatus {
      if mr.jobsStatus[j] == 0 { // not assigned
        mr.jobsStatus[j] = 1 // mark the job as running
        mr.Workers[w].splitId = j
        go mr.assignJob(w, j)
        assigned = true
        break
      }
    }
    if assigned {
      continue
    }

    // all jobs have been assigned, check if they finished
    allFinished := true
    for j := range mr.jobsStatus {
      if mr.jobsStatus[j] != 2 { // still running
        allFinished = false
        break
      }
    }
    if allFinished {
      break
    } // else, jobs still running, continue wait for response
  }

  return mr.KillWorkers()
}

