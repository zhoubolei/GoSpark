package spark

import (
  "fmt"
  "net/rpc"
)

import "crypto/rand"
import "strconv"
import "math/big"

func nrand() string {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return strconv.FormatInt(x, 10)
}


const (
  LineCount = "LineCount"
  ReadHDFSSplit = "ReadHDFSSplit"
  GetSplit = "GetSplit"
)

type Yielder chan interface{}
type ReducerFn func(yield Yielder, partition int) interface{}   // for scheduler to run 

type MapperFunc func(interface{}) interface{}
type PartitionMapperFunc func(Yielder) Yielder
type FlatMapperFunc func(interface{}) []interface{}
type ReducerFunc func(interface{}, interface{}) interface{}
type FilterFunc func(interface{}) bool
type LoopFunc func(interface{})




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
  HDFSSplitID int
  InputID string
  OutputID string
  Function string
}

type DoJobReply struct {
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



