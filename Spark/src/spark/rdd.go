package spark

import (

)

type RDDChain struct {

}

type RDD struct {
  isFromFile int
  inputFile string
   
  partitions []string
  function string
  OpId string  // map, join, ..., load file
  //Dependency
}