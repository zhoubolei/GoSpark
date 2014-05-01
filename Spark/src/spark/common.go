package spark

import (
  "fmt"
  "net/rpc"
)

const (
  LineCount = "LineCount"
)

type JobType string

// RPC arguments and replies.  Field names must start with capital letters,
// otherwise RPC will break.

// for initial register worker -> master

type RegisterArgs struct {
  Worker string
  NCore int
}

type RegisterReply struct {
  OK bool
}

// for shut down master -> worker

type ShutdownArgs struct {
}

type ShutdownReply struct {
  Njobs int
  OK bool
}

// assign job master -> worker

type DoJobArgs struct {
  Operation JobType
  File string
  SplitID int
}

type DoJobReply struct {
  OK bool
}


// fetch job result from workers

type FetchArgs struct {
  File string
  SplitID int
}

type FetchReply struct {
  Result interface{}
  OK bool
}


// sends an RPC

func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("tcp", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


type Yielder chan interface{}
