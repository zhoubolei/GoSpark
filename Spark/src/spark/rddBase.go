package spark

import (

)

// derive from this base RDD to have more types of RDD map, join, ..., HDFS file
type BaseRDD struct {
    id              int64
    context         *Context
    //partitionType   PartitionType // HashPartition / RangePartition
    splits          []Split
    function        Function   
    dependency      Dependency
    metadata        Metadata  // partitioning scheme and data placement
}

///////////////////////////////////// RDD Actions ////////
func (rdd *BaseRDD) Collect() []interface{} {
    DPrintf("Do collect\n");
    yielders := r.context.runRoutine(
    
    )
}

