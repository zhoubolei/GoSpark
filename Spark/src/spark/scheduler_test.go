package spark

import (
  "testing"
  "fmt"
  "math/rand"
  "strings"
  "strconv"
  "encoding/gob"
)

func (f *UserFunc) MapLineToAnotherLine(line interface{}) interface{} {
  return line.(KeyValue).Value.(string) + "aaaaaa"
}



func TestBasicMappingAndCollect(t *testing.T) {
  gob.Register(CenterCounter{})
  gob.Register(VectorVector{})
  c := NewContext("kmeans")
  defer c.Stop()
  
  //D := 4
  //K := 16
  //MIN_DIST := 0.01

  
  pointsText := c.TextFile("hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv")
  //pointsText := c.TextFile("hdfs://localhost:54310/user/kmean_data.txt"); pointsText.name = "pointsText"
  //pointsText := c.TextFile("hdfs://localhost:54310/user/hduser/testSplitRead.txt"); pointsText.name = "pointsText"
  
  pointsAppend := pointsText.Map("MapLineToAnotherLine").Cache();  pointsText.name = "pointsAppend"
  
  pointsAppendCollected := pointsAppend.Collect()    
  fmt.Printf("len(pointsAppendCollected):%v\n", len(pointsAppendCollected))  
  
  collected := []string{}     
  for i:=0; i<len(pointsAppendCollected); i++ {
    collected = append(collected, pointsAppendCollected[i].(string))
  }
  
  fmt.Println("collected:", collected)
}


