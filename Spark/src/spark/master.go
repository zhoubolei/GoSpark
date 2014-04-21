package spark

import "container/list"
import "net"

type WorkerInfo struct {
  address string
  nCore int
}

type JobCompleteStatus struct{
  workerId string
  jobId int
  OK bool
}


type Master struct {
  file string  // Name of input file
  MasterAddress string
  registerChannel chan string
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

func MakeMapReduce(nmap int, nreduce int,
                   file string, master string) *Master {
  mr := Master{}
  return &mr
}

/*
func (mr *Master) runJobThread(workerId string, jobId int, jobType JobType){
}

func (mr *Master) assignJobs(jobType JobType) {

}
// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *Master) KillWorkers() *list.List {

}*/




