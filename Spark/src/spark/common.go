package spark

import (
  "fmt"
  "log"
  "flag"
  "net/rpc"
  "encoding/gob"
  "math/big"
  "crypto/rand"
  "crypto/md5"
  "bytes"
  "strconv"
)

var Debug = flag.Bool("debug", false, "spark debug mode")

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if *Debug {
    log.Printf(format, a...)
  }
  return
}

func nrand() string {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return strconv.FormatInt(x, 10)
}

func hash(v interface{}) int64 {
  if v == nil {
    return 0
  }
  buf := new(bytes.Buffer)
  enc := gob.NewEncoder(buf)
  enc.Encode(v)
  h := md5.New()
  h.Write(buf.Bytes())
  arr := h.Sum(nil)
  sum := int64(0)
  for _, x := range arr {
    sum *= 256
    sum += int64(x)
  }
  return sum
}

const (
  ReadHDFSSplit   = "ReadHDFSSplit"
  HasSplit        = "HasSplit"
  GetSplit        = "GetSplit"
  DelSplit        = "DelSplit"
  Count           = "Count"
  MapJob          = "MapJob"
  FlatMapJob      = "FlatMapJob"
  MapValuesJob    = "MapValuesJob"
  FilterJob       = "FilterJob"
  SampleJob       = "SampleJob"
  HashPartJob     = "HashPartJob"
  ReduceJob       = "ReduceJob"
  ReduceByKeyJob  = "ReduceByKeyJob"
  JoinJob         = "JoinJob"
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
  Running int
  MemUse uint64
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
  HDFSFile string
  HDFSSplitID int
  InputID string
  InputIDs []Split
  InputIDs2 []Split // for Join
  OutputID string
  OutputIDs []Split
  Function string
  SampleN int
  SampleSeed int64
  Data interface{} // in case other inputs are needed
}

type DoJobReply struct {
  Result interface{}
  Lines []interface{}
  OK bool
  NeedSplits []string
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



