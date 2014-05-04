package spark

import (
  "sync"
)

// this base RDD to have more types of RDD map, join, ..., HDFS file
type RDD struct {
  id              string
  ctx             *Context
  length          int       // number of splits
  splits          []*Split
  dependType      string
  splitType       string
  operationType   string
  fnName          string    // function name for map, reduce ...
  fnData          interface{}    // function name for map, reduce ...
  filePath        string
  shouldCache     bool
    
  prevRDD1        *RDD
  prevRDD2        *RDD
  
  isTarget        bool
  shuffleN        int
  shuffleSplits   [][]*Split
  shuffleMu       sync.Mutex              
}

func makeRDD(ctx *Context, length int, dependType string, splitType string, operationType string, filePath string,
             fnName string, prevRDD1 *RDD, prevRDD2 *RDD) *RDD{
  r := RDD{}
  r.ctx = ctx
  r.length = length;
  r.splits = make([]*Split, length)
  for i:=0; i<length; i++ {
    r.splits[i] = makeSplit()
  }
  r.dependType = dependType
  r.splitType = splitType
  r.operationType = operationType
  r.fnName = fnName
  r.filePath = filePath
  r.prevRDD1 = prevRDD1
  r.prevRDD2 = prevRDD2
  return &r
}

func (r *RDD) Map(fnName string) *RDD {
  newRdd := makeRDD(r.ctx, r.length, Narrow, r.splitType, Map, "", fnName, r, nil)
  return newRdd
}

func (r *RDD) MapWithData(fnName string, fnData interface{}) *RDD {
  newRdd := makeRDD(r.ctx, r.length, Narrow, r.splitType, Map, "", fnName, r, nil)
  newRdd.fnData = fnData
  return newRdd
}

func (r *RDD) FlatMap(fnName string) *RDD {
  newRdd := makeRDD(r.ctx, r.length, Narrow, r.splitType, FlatMap, "", fnName, r, nil)
  return newRdd
}

func (r *RDD) ReduceByKey(fnName string) *RDD {
  newRdd := makeRDD(r.ctx, r.length, Wide, HashPartition, ReduceByKey, "", fnName, r, nil)
  return newRdd
}

func (r *RDD) Filter(fnName string) *RDD {
  newRdd := makeRDD(r.ctx, r.length, Narrow, r.splitType, Filter, "", fnName, r, nil)
  return newRdd
}
///////////////////////////////////// RDD Actions ////////

func (r *RDD) Reduce(fnName string) interface{} {
  x := r.ctx.scheduler.computeRDD(r, Reduce, fnName);
  ret := x[0]
  
  // Todo: get function by name
  f := func(interface{}, interface{}) interface{}{ return nil}//
  for i:=1; i<len(x); i++ {
    ret = f(ret, x[i])
  }
  return ret
}

func (r *RDD) Collect() []interface{} {
  x := r.ctx.scheduler.computeRDD(r, Collect, "")
  ret := x[0].([]interface{})
  
  for i:=1; i<len(x); i++ {
    ret = append(ret, x[i].([]interface{}))
  }
  return ret
}

func (r *RDD) Count() int64 {
  x := r.ctx.scheduler.computeRDD(r, Count, "")
  ret := x[0].(int64)
  
  for i:=1; i<len(x); i++ {
    ret += x[i].(int64);
  }
  return ret;
}

// Memory options
func (r *RDD) Cache() *RDD {
  r.shouldCache = true
  return r;
}

// DependType
const (
  Narrow = "Narrow"
  Wide =   "Wide"  // require data from all parent partitions to be available
)

// OperationType
const ( 
  Map =         "Map"
  FlatMap =     "FlatMap"
  Reduce =      "Reduce"
  ReduceByKey = "ReduceByKey"
  Filter      = "Filter"
  Collect     = "Collect"
  HDFSFile    = "HDFSFile"
)  

// SplitTypes
const ( 
  HashPartition = "HashPartition"
  RangePartition = "RangePartition"
)  



