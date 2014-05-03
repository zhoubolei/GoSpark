package spark

import (
  "testing"
  "fmt"
  "log"
  "os"
  "bufio"
  "hadoop"
  "strings"
  "strconv"
  "time"
)

func TestBasicMaster(t *testing.T) {
  fmt.Printf("Test: Basic Master Line Count...\n")

  // master ip & port
  f, err := os.Open("config.txt")
  if err != nil {
    log.Fatal(err)
  }
  s := bufio.NewScanner(f)
  s.Scan()
  master_ip := s.Text()
  s.Scan()
  master_port := s.Text()
  fmt.Printf("master ip %s port %s\n", master_ip, master_port)

  mr := MakeMaster(master_ip, master_port)
  file := "hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv"
  nsplits := hadoop.GetSplitInfo(file).Len()
  i := 0
  for i < nsplits {
    workers := mr.WorkersAvailable()
    for w := range workers {
      args := DoJobArgs{Operation:ReadHDFSSplit, HDFSFile:file, HDFSSplitID:i, OutputID:strings.Join([]string{file, strconv.Itoa(i)}, "-")}
      var reply DoJobReply
      ok := mr.AssignJob(w, &args, &reply)
      if ok {
        i++
        if i >= nsplits {
          break
        }
      } else {
        time.Sleep(10 * time.Millisecond)
      }
    }
  }
  mr.Shutdown()
  // TODO check
  //check(t, mr.file)
  //checkWorker(t, mr.stats)
  //cleanup(mr)
  //fmt.Printf("  ... File Read Passed\n")
}
