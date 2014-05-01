package main 

import (

)

type Scheduler interface {
    start()
    clear()
    stop()
    runRoutine(rdd RDD, partitions []int, rn ReducerFn) []Yielder
}

