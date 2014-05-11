package spark

import (
  "testing"
  "fmt"
  "math/rand"
  "strings"
  "strconv"
  "encoding/gob"
  "math"
  "os"
  "flag"
)

// Format [PicIndex],[CategoryIndex],[feature1],[feature2],[feature3],[feature4],[feature5]...
func (f *UserFunc) MapLineToFloatVectorCatCSV(line interface{}) interface{} {
  fieldTexts := strings.FieldsFunc(line.(KeyValue).Value.(string), func(c rune) bool { return c == ',' })
  
  vecs := make(Vector, len(fieldTexts)-1)
  vecs[0] = 1
  for i := range vecs {
    if i>0 {      
      vecs[i], _ = strconv.ParseFloat(fieldTexts[i+1], 64)
    }
  }
  
  y := 0.0
  if cat, _ := strconv.Atoi(fieldTexts[1]); cat < 150 { // is of some category or not
    y = 1.0
  }
  return KeyValue{y, vecs}
}

func (f *UserFunc) MapToVectorGradient(xy interface{}, wInterface interface{}) interface{} {
  y := xy.(KeyValue).Value.(KeyValue).Key.(float64)
  x := xy.(KeyValue).Value.(KeyValue).Value.(Vector)
  w := wInterface.(KeyValue).Value.(*Vector)
  
  //DPrintf("len(x)=%v len(w)=%v\n", len(x), len(*w))
  grad := x.Multiply(1/(1+math.Exp(w.Dot(x)))-y)
  return grad
}

func (f *UserFunc) RedToOneGradient(xInterface, yInterface interface{}) interface{} {
  var x, y Vector
  if _, ok := xInterface.(KeyValue).Value.(Vector); ok {
    x = xInterface.(KeyValue).Value.(Vector)
  } else {
    x = *(xInterface.(KeyValue).Value.(*Vector))
  }
  
  if _, ok := yInterface.(KeyValue).Value.(Vector); ok {
    y = yInterface.(KeyValue).Value.(Vector)
  } else {
    y = *(yInterface.(KeyValue).Value.(*Vector))
  }
  return (x).Plus(y)
}

func (f *UserFunc) MapToLRLabelAndTrueLabel(xy interface{}, wInterface interface{}) interface{} {
  y := xy.(KeyValue).Value.(KeyValue).Key.(float64)
  x := xy.(KeyValue).Value.(KeyValue).Value.(Vector)
  w := wInterface.(KeyValue).Value.(*Vector)
  
  yp := (1/(1+math.Exp(-(w.Dot(x)))))
  return KeyValue{yp, y}
}

var Local = flag.Bool("local", false, "Run on vision server")
var Big = flag.Bool("big", false, "use big data")

func TestLR(t *testing.T) {
  c := NewContext("LR")
  defer c.Stop()
  
  
  gob.Register([]Vector{})
  
  D := 4096 +1
  DD := min(10,D)  // get first few elements to print out
  
  alpha := 0.1     // alpha of gradient descend
  
  w := make(Vector, D)
  for i := range w {
    w[i] = rand.Float64()
  }
  
  hadoopPath := ""
  if *Big {
    hadoopPath = "/user/featureSUN397_large.csv"
  } else {
    hadoopPath = "/user/featureSUN397_combine.csv"
  }
  hdfsServer := ""
  if *Local {
    hdfsServer = "hdfs://localhost:54310" 
  } else {
    hdfsServer = "hdfs://vision24.csail.mit.edu:54310" 
  }
  fileURI := fmt.Sprintf("%s%s", hdfsServer, hadoopPath)
  //pointsText := c.TextFile("hdfs://localhost:54310/user/featureSUN397_combine_smallLR.csv"); pointsText.name = "pointsText"
  //pointsText := c.TextFile("hdfs://vision24.csail.mit.edu:54310/user/featureSUN397_combine.csv"); pointsText.name = "pointsText"
  pointsText := c.TextFile(fileURI); pointsText.name = "pointsText"
  points := pointsText.Map("MapLineToFloatVectorCatCSV").Cache();  points.name = "points"
  
  fmt.Printf("Initial w[0:DD]=%v\n", w[0:DD])
  for i:=0; i<10; i++ {
    fmt.Println("Iter:", i)
	  mappedPoints := points.MapWithData("MapToVectorGradient", w); mappedPoints.name = "mappedPoints"  
    gradInterface := mappedPoints.Reduce("RedToOneGradient")
    w = w.Minus((gradInterface.(Vector)).Multiply(alpha))
    fmt.Printf("w[0:DD]=%v\n", w[0:DD])
  }
  Compare := points.MapWithData("MapToLRLabelAndTrueLabel", w).Collect();
  
  fout, _ := os.Create("LROutput-CompareLabels.txt")
  defer fout.Close()
  for i := 0; i < len(Compare); i++ {
    fout.WriteString( fmt.Sprintf("%.3f %.3f\n", Compare[i].(KeyValue).Key.(float64), Compare[i].(KeyValue).Value.(float64)) ) 
  }
}


