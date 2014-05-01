package spark

import (

)

// this is a rdd that is derived from some previous RDD, like map 
// so it needs to remember the previoius RDD
type DerivedRDD struct {
    BaseRDD
    previous RDD   // dependency
}

func (d *DerivedRDD) init(prevRdd, prototype RDD) {
    d.BaseRDD.init(prevRdd.getContext(), prototype)
    d.previous = prevRdd
    d.length = prevRdd.len()
    d.splits = prevRdd.getSplits()
}
