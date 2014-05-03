package spark 

import (

)

type Scheduler interface {
    //start()
    //clear()
    //stop()
    runRoutine(rdd* RDD, rn string) []Yielder
}

type DistScheduler struct {
    master *Master
}

func (d *DistScheduler) runRoutine(rdd* RDD, rn string) []Yielder {
    // This function is called when user triggers an action or before an RDD go into a wide dependency process like reduceByKey
    // The reduce function rn collect all data "in a split" from an RDD with a yielder and do some reduce process
    // []Yielder is an array of splits' product after applying rn on each split
    //nOutputSplit = nPartitions
    /*nOutputSplit := rdd.len()
    yielders := make([]Yielder, nOutputSplit)
    for i=0; i<nOutputSplit; i++ {
      split := rdd.getSplit(i)
      yielders[i] = make(chan interface{})
      
      go func(yielder Yielder, splitInd int) {  
        // 1. check if the previous depended split is done
        rdd.compute
        // 2. ask master to send the job of "applying rn to the split" to worker
        JobInfo{rdd, splitInd, rn}
        d.master.sendJob(rdd, splitInd, rn, input, output)
        yielder <- rn(partition)
        close(yielder)
      }(yielders[i], i)
    }*/
    
    return nil
}

func NewDistScheduler() Scheduler {
    scheduler := &DistScheduler{}
    // TODO: get address and port name from somewhere
    address := "vision24";
    port := ":10000"; 
    scheduler.master = MakeMaster(address, port)   
    
    return scheduler
}
