package spark
import "testing"
import "fmt"
import "strings"
import "strconv"
import "time"

func TestFileRead(t *testing.T) {
  fmt.Printf("Test: Line Count...\n")
  mr := MakeMaster("hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv", "LineCount", "127.0.0.1", ":10000") // TODO: IP address?
  for i := 0; i < 5; i++ {
    portstr := strings.Join([]string{":", strconv.Itoa(5001 + i)}, "")
    go RunWorker(mr.MasterAddress, mr.MasterPort, "127.0.0.1", portstr, -1)
  }
  // Wait until MR is done
  <- mr.DoneChannel
  // TODO check
  //check(t, mr.file)
  //checkWorker(t, mr.stats)
  //cleanup(mr)
  //fmt.Printf("  ... File Read Passed\n")
}

func TestPeterkty(t *testing.T){
	c := make(chan int, 1)
	go func(c chan int){
	   for s:= range c {
	      fmt.Printf("%v 1\n", s)
	      time.Sleep(100 * time.Millisecond)
	   }
	} (c)
	c <- 1
	c <- 1
	close(c)
}