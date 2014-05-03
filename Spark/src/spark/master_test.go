package spark

import (
  "testing"
  "fmt"
  "log"
  "os"
  "bufio"
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

  mr := MakeMaster("hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv", "ReadHDFSSplit", master_ip, master_port)
  <- mr.DoneChannel
  // TODO check
  //check(t, mr.file)
  //checkWorker(t, mr.stats)
  //cleanup(mr)
  //fmt.Printf("  ... File Read Passed\n")
}
