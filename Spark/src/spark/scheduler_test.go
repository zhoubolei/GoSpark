package spark

import (
  "testing"
  "fmt"
  "math/rand"
  "strings"
  "strconv"
)

func (f *UserFunc) MapLineToAnotherLine(line interface{}, userData interface{}) interface{} {
  //fieldTexts := strings.Fields(line.(string))
  
  //return line.(string) + "aaaaaa"
  return line.(KeyValue).Value.(string) + "aaaaaa"
}

func (f *UserFunc) MapLineToFloatVectorCSV(line interface{}) interface{} {
  //fieldTexts := strings.Fields(line.(string))
  fieldTexts := strings.FieldsFunc(line.(string), func(c rune) bool { return c == ',' })
  
  vecs := make(Vector, len(fieldTexts)-1)
  for i := range vecs[1:] {
    vecs[i-1], _ = strconv.ParseFloat(fieldTexts[i-1], 64)
  }
  return vecs
}

func (f *UserFunc) MapLineToFloatVector(line interface{}) interface{} {
  fieldTexts := strings.Fields(line.(string))
  
  vecs := make(Vector, len(fieldTexts))
  for i := range vecs {
    vecs[i], _ = strconv.ParseFloat(fieldTexts[i], 64)
  }
  return vecs
}

type CenterCounter struct {
    X     Vector
    Count int
}

func (f *UserFunc) AddCenterWCounter(x, y interface{}) interface{} {
  cc1 := x.(CenterCounter)
  cc2 := y.(CenterCounter)
  return CenterCounter{
    X:     cc1.X.Plus(cc2.X),
    Count: cc1.Count + cc2.Count,
  }
}

func (f *UserFunc) AvgCenter(x, y interface{}) interface{} {
  cc1 := x.(CenterCounter)
  cc2 := y.(CenterCounter)
  return CenterCounter{
    X:     cc1.X.Plus(cc2.X),
    Count: cc1.Count + cc2.Count,
  }
}


func TestBasicMappingAndCollect(t *testing.T) {
  c := NewContext("kmeans")
  defer c.Stop()
  
  //D := 4
  //K := 16
  //MIN_DIST := 0.01

  
  //pointsText := c.TextFile("hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv")
  //pointsText := c.TextFile("hdfs://localhost:54310/user/kmean_data.txt"); pointsText.name = "pointsText"
  pointsText := c.TextFile("hdfs://localhost:54310/user/hduser/testSplitRead.txt"); pointsText.name = "pointsText"
  
  pointsAppend := pointsText.Map("MapLineToAnotherLine").Cache();  pointsText.name = "pointsAppend"
  
  pointsAppendCollected := pointsAppend.Collect()    
  fmt.Printf("len(pointsAppendCollected):%v\n", len(pointsAppendCollected))  
  
  collected := []string{}     
  for i:=0; i<len(pointsAppendCollected); i++ {
    collected = append(collected, pointsAppendCollected[i].(string))
  }
  
  fmt.Println("collected:", collected)
}


func TestBasicScheduler(t *testing.T) {
  c := NewContext("kmeans")
  defer c.Stop()
  
  D := 4
  K := 3
  //MIN_DIST := 0.01

  centers := make([]Vector, K)
  for i := range centers {
    center := make(Vector, D)
    for j := range center {
      center[j] = rand.Float64()
    }
    centers[i] = center
  }
  fmt.Println(centers)
  
  //pointsText := c.TextFile("hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv")
  pointsText := c.TextFile("hdfs://localhost:54310/user/kmean_data.txt"); pointsText.name = "pointsText"
  //pointsText := c.TextFile("hdfs://localhost:54310/user/hduser/testSplitRead.txt"); pointsText.name = "pointsText"
  
  points := pointsText.Map("MapLineToFloatVector").Cache();  points.name = "points"
  
  // run one kmeans iteration
  // points (x,y) -> (index of the closest center, )
  mappedPoints := points.MapWithData("ClosestCenter", centers); mappedPoints.name = "mappedPoints"   
  sumCenters := mappedPoints.ReduceByKey("AddCenterWCounter") ; sumCenters.name = "sumCenters"  
  newCenters := sumCenters.Map("AvgCenter")                   ; newCenters.name = "newCenters"  
  newCentersCollected := newCenters.Collect()                 
  for i:=0; i<len(newCentersCollected); i++ {
    centers[i] = newCentersCollected[i].(Vector)
  }
  
  fmt.Println("Final Centers:", centers)
}
