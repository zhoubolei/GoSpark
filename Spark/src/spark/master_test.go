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

func make_master() *Master {
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

  return MakeMaster(master_ip, master_port)
}

func TestBasicMaster(t *testing.T) {
  fmt.Printf("Test: Basic Master Line Count...\n")
  mr := make_master()
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

func TestMasterMR(t *testing.T) {
  fmt.Printf("Test: Master MapReduce Line Count...\n")
  mr := make_master()
  file := "hdfs://vision24.csail.mit.edu:54310/user/kmean_data.txt"
  nsplits := hadoop.GetSplitInfo(file).Len()
  workers := mr.WorkersAvailable()
  for len(workers) < 1 {
    time.Sleep(1 * time.Second)
    workers = mr.WorkersAvailable()
  }
  var w string
  for wk := range workers {
    w = wk
    break
  }
  reduce_in := make([]Split, nsplits)
  for i := 0; i < nsplits; i++ {
    // read from HDFS
    read_out := strings.Join([]string{file, "read", strconv.Itoa(i)}, "-")
    read_args := DoJobArgs{Operation:ReadHDFSSplit, HDFSFile:file, HDFSSplitID:i, OutputID:read_out}
    var read_reply DoJobReply
    mr.AssignJob(w, &read_args, &read_reply)
    // perform map
    map_out := strings.Join([]string{file, "map", strconv.Itoa(i)}, "-")
    map_args := DoJobArgs{Operation:MapJob, InputID:read_out, OutputID:map_out, Function:"LineCount"}
    var map_reply DoJobReply
    mr.AssignJob(w, &map_args, &map_reply)
    reduce_in[i] = Split{SplitID:map_out, Hostname:""}
  }
  // perform reduce
  reduce_out := strings.Join([]string{file, "reduce"}, "-")
  reduce_args := DoJobArgs{Operation:ReduceByKey, InputIDs:reduce_in, OutputID:reduce_out, Function:"SumInt"}
  var reduce_reply DoJobReply
  mr.AssignJob(w, &reduce_args, &reduce_reply)
/*
  get_args := DoJobArgs{Operation:GetSplit, InputID:[]string{reduce_out}}
  var get_reply DoJobReply
  mr.AssignJob(w, &get_args, &get_reply)
*/

  mr.Shutdown()
  // TODO check
  //check(t, mr.file)
  //checkWorker(t, mr.stats)
  //cleanup(mr)
  //fmt.Printf("  ... File Read Passed\n")
}

func TestMasterNotFound(t *testing.T) {
  fmt.Printf("Test: Master File Not Found...\n")
  mr := make_master()
  workers := mr.WorkersAvailable()
  for len(workers) < 1 {
    time.Sleep(1 * time.Second)
    workers = mr.WorkersAvailable()
  }
  var w string
  for wk := range workers {
    w = wk
    break
  }
  inputs := []Split{Split{SplitID:"asdf", Hostname:""}, Split{SplitID:"qwer", Hostname:""}}
  args := DoJobArgs{Operation:ReduceByKey, InputIDs:inputs, OutputID:"yui", Function:"SumInt"}
  var reply DoJobReply
  mr.AssignJob(w, &args, &reply)

  mr.Shutdown()
  // TODO check
  //check(t, mr.file)
  //checkWorker(t, mr.stats)
  //cleanup(mr)
  //fmt.Printf("  ... File Read Passed\n")
}
