package spark

import (

)

// derive from this base RDD to have more types of RDD map, join, ..., HDFS file
type BaseRDD struct {
    id              int64
    context         *Context
    length          int       // number of splits
    splits          []Split
    
    //partitionType   PartitionType // HashPartition / RangePartition
    function        Function   
    dependency      Dependency
    metadata        Metadata  // partitioning scheme and data placement
}

////////////////////////////////////////
type SplitID int64
type Partitions struct{

}

type Function struct{

}
type Metadata struct{

}
type ParentIters struct{
}

// List nodes where partition p can be accessed faster due to data locality
func (rdd *BaseRDD) PreferredLocations (p SplitID) {

}


func (rdd *BaseRDD) init(ctx *Context, prototype RDD) {
}

func (rdd *BaseRDD) len() int {
    return rdd.length
}

func (rdd *BaseRDD) getContext() *Context {
    return rdd.context
}

func (rdd *BaseRDD) getSplits() []Split {
    return rdd.splits
}

func (rdd *BaseRDD) getSplit(index int) Split {
    return rdd.splits[index]
}


func (rdd *BaseRDD) FlatMap(f FlatMapperFunc) RDD {
    // TODO
    return nil
}

func (rdd *BaseRDD) Map(f MapperFunc) RDD {
    // TODO
    return nil
}

func (r *BaseRDD) Reduce(fn ReducerFunc) interface{} {
    return nil
}

func (rdd *BaseRDD) ReduceByKey(fn ReducerFunc) RDD {
    // TODO
    return nil
}

func (rdd *BaseRDD) Filter(f FilterFunc) RDD {
    //return newFilteredRDD(rdd.prototype, f)
    return nil
}
///////////////////////////////////// RDD Actions ////////
func (rdd *BaseRDD) Collect() []interface{} {
    DPrintf("Do collect\n");
    //yielders := rdd.context.runRoutine();
    return nil;
}

func (rdd *BaseRDD) Count() int64 {
    return 0;
}

