package spark 

import (

)

type Scheduler interface {
    //start()
    //clear()
    //stop()
    runRoutine(rdd RDD, partitions []int, rn ReducerFn) []Yielder
}

type DistScheduler struct {
    master *Master
}

func (d *DistScheduler) runRoutine(rdd RDD, partitions []int, rn ReducerFn ) []Yielder {
    //TODO
    return nil
}

func NewDistScheduler() Scheduler {
    scheduler := &DistScheduler{}
    // TODO: get address and port name from somewhere
    address := "vision24";
    port := "5000"; 
    scheduler.master = MakeMaster("", "", address, port)   
    
    return scheduler
}
