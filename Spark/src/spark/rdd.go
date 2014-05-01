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

////////////////////////////////////////
type PartitionID int64
type Partitions struct{

}
////////////////////////////////////////

type Function struct{

}
////////////////////////////////////////
type Metadata struct{

}
////////////////////////////////////////
type ParentIters struct{
}
////////////////////////////////////////




// Return a list of Partition objects
func (rdd *RDD) partitions (){

}

// List nodes where partition p can be accessed faster due to data locality
func (rdd *RDD) PreferredLocations (p SplitID) {

}

// Return a list of dependencies
func (rdd *RDD) Dependencies() Dependency{
  return rdd.Dependency
}

// Compute the elements of partition p given iterators for its parent partitions
func (rdd *RDD) Iterator(p PartitionID, parentIters ParentIters) {

}

// Return metadata specifying whether the RDD is hash/range partitioned
func (rdd *RDD) Partitioner() {

}

///////////////////////////////////// function used in computation
func (rdd *RDD) Persist() {

}

///////////////////////////////////// RDD Transformations ////////

func (rdd *RDD) Map() {

}

func (rdd *RDD) ReduceByKey() {

}

///////////////////////////////////// RDD Transformations ////////
func (rdd *RDD) Reduce() {

}