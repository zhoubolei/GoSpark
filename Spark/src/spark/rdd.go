package spark

import (

)


type RDD interface {
    Map(f MapperFunc) RDD
    FlatMap(f FlatMapperFunc) RDD
    Filter(f FilterFunc) RDD
    ReduceByKey(fn ReducerFunc) RDD
    
    Reduce(fn ReducerFunc) interface{}
    Collect() []interface{}
    Count() int64
    
    
    getContext() *Context
    //compute(split Split) Yielder
    len() int
    getSplits() []Split
    getSplit(int) Split
}



type DependType string

const (
  Narrow = "Narrow"
  Wide = "Wide"  // require data from all parent partitions to be available
)

type Dependency struct {
  DependType DependType  // narrow/wide
}

type PartitionType string
const ( 
  HashPartition = "HashPartition"
  RangePartition = "RangePartition"
)  

type OperationType string
const ( 
  Map = "Map"
  Reduce = "Reduce"
)  



