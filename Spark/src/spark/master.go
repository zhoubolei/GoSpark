package spark

import (
  "container/list"
  "net"
  "net/rpc"
  "log"
  "sync"
  "encoding/gob"
  "time"
)

type WorkerInfo struct {
  address string // addr:port of the worker, e.g. "127.0.0.1:1234"
  nCore int      // for load-balancing on workers
  running int // number of jobs currently running
  memUse uint64 // amount of memory currently using
}

type Master struct {
  MasterAddress string // e.g. "127.0.0.1"
  MasterPort string // e.g. ":1234"
  registerChannel chan RegisterArgs
  alive bool
  l net.Listener
  stats *list.List // TODO use this in test

  // Map of registered workers that you need to keep up to date
  mu sync.RWMutex
  workers map[string]WorkerInfo
}

func MakeMaster(ip string, port string) *Master {
  gob.Register(KeyValue{})
  gob.Register(Pair{})
  register_types() // register custom types

  mr := Master{}
  mr.MasterAddress = ip
  mr.MasterPort = port
  mr.alive = true
  mr.registerChannel = make(chan RegisterArgs)
  mr.workers = make(map[string]WorkerInfo)
  mr.StartRegistrationServer()
  return &mr
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *Master) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.workers {
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
  //DPrintf("Register: %v", args)
  res.OK = true

  // update worker information
  mr.mu.Lock()
  mr.workers[args.Worker] = WorkerInfo{address:args.Worker, nCore:args.NCore, running:args.Running, memUse:args.MemUse}
  mr.mu.Unlock()
  return nil
}

func (mr *Master) Shutdown() error {
  DPrintf("Shutdown: registration server\n")
  mr.stats = mr.KillWorkers()
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
        DPrintf("RegistrationServer: accept error %s", err)
        break
      }
    }
    DPrintf("RegistrationServer: done\n")
  }()

  DPrintf("RegistrationServer: ready")
}

func (mr *Master) WorkersAvailable() map[string]WorkerInfo {
  mr.mu.RLock()
  defer mr.mu.RUnlock()
  // because mr.workers is a map, so the returned map is the of same pointer to what master have.
  // so some concurrency issue may happen. Better manually do a deep copy of mr.workers and return that copy.
  m := make(map[string]WorkerInfo)
  for w, i := range mr.workers {
    m[w] = i
  }
  return m
}

func (mr *Master) assign_to_worker(w string, args *DoJobArgs, reply *DoJobReply) bool {
  ok := call(w, "Worker.DoJob", args, reply)
  trial := 0
  for !ok && trial < 10 {
    DPrintf("RPC failed, try again")
    time.Sleep(time.Second)
    ok = call(w, "Worker.DoJob", args, reply)
    trial++
  }
  if !ok { // RPC fails, need to assign current job to another worker
    DPrintf("worker %s connection lost", w)
    mr.mu.Lock()
    delete(mr.workers, w) // remove from workers pool
    mr.mu.Unlock()
    return false
  } else { // current job is done, ready for the next job
    DPrintf("worker %s args %v reply %v", w, args, reply)
    return true
  }
}

func (mr *Master) assign_if_capable(w string, args *DoJobArgs, reply *DoJobReply) bool {
  mr.mu.RLock()
  info, avail := mr.workers[w]
  mr.mu.RUnlock()
  if !avail { // not available at all
    return false
  }
  if info.running >= info.nCore { // all cores are busy
    return false
  }
  // have idle cores, good to assign
  return mr.assign_to_worker(w, args, reply)
}

func (mr *Master) find_least_load(workersToConsider []string) string {
  min_load := float32(-1)
  min_worker := ""
  for _, w := range workersToConsider {
    mr.mu.RLock()
    info, avail := mr.workers[w]
    mr.mu.RUnlock()
    if !avail {
      continue
    }
    load := float32(info.running) / float32(info.nCore)
    if min_load < 0 || load < min_load {
      min_load = load
      min_worker = w
    }
  }
  return min_worker
}

func (mr *Master) AssignJob(workersPreferred []string, force bool, args *DoJobArgs, reply *DoJobReply) (bool, string) {
  // use nCore to assign nCore jobs to select workers, return the worker chosen

  // first try the preferred workers if any of them is capable
  for _, w := range workersPreferred {
    if mr.assign_if_capable(w, args, reply) {
      return true, w
    }
  }
  // then try all workers if any of them is capable
  all := mr.WorkersAvailable()
  if !force {
    for w := range all {
      if mr.assign_if_capable(w, args, reply) {
        return true, w
      }
    }
  }
  // if still fail, pick the preferred worker with lightest load
  w := mr.find_least_load(workersPreferred)
  // if no preferred workers are available, pick any worker with the lightest load
  if w == "" {
    if !force {
      all = mr.WorkersAvailable() // refresh
      all_avail := make([]string, 0)
      for w_avail := range all {
        all_avail = append(all_avail, w_avail)
      }
      w = mr.find_least_load(all_avail)
    }
    // if no any worker available by this time
    if w == "" {
      return false, ""
    }
  }
  // assign to this worker with lightest load
  if mr.assign_to_worker(w, args, reply) {
    return true, w
  } else {
    return false, ""
  }
}

