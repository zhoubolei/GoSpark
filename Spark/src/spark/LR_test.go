package spark
/*
import (
  "testing"
  "fmt"
  "math/rand"
  "strings"
  "strconv"
  "encoding/gob"
  "os"
)
/
// Format [PicIndex],[CategoryIndex],[feature1],[feature2],[feature3],[feature4],[feature5]...
func (f *UserFunc) MapLineToFloatVectorCSVWithCat(line interface{}) interface{} {
  fieldTexts := strings.FieldsFunc(line.(KeyValue).Value.(string), func(c rune) bool { return c == ',' })
  
  vecs := make(Vector, len(fieldTexts)-2)
  for i := range vecs {
    vecs[i], _ = strconv.ParseFloat(fieldTexts[i+2], 64)
  }
  return vecs
}


func TestLR(t *testing.T) {
  c := NewContext("LR")
  defer c.Stop()
  
}
*/

