package spark

import (

)

// derive from here to have different kinds of HashPartition / RangePartition

type Split interface {
    getIndex() int
}
