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

// to add fault tolerance (worker temp RPC error) to master tests
func get_first_worker(mr *Master) string {
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
  return w
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

func TestMasterMRLineCount(t *testing.T) {
  fmt.Printf("Test: Master MapReduce Line Count...\n")
  mr := make_master()
  file := "hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv"
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
    for !mr.AssignJob(w, &read_args, &read_reply) { // if worker fails, roughly, wait for another available worker
      w = get_first_worker(mr)
    }

    // perform map
    map_out := strings.Join([]string{file, "map", strconv.Itoa(i)}, "-")
    map_args := DoJobArgs{Operation:MapJob, InputID:read_out, OutputID:map_out, Function:"LineCount"}
    var map_reply DoJobReply
    for !mr.AssignJob(w, &map_args, &map_reply) { // if worker fails, roughly, wait for another available worker
      w = get_first_worker(mr)
    }

    reduce_in[i] = Split{SplitID:map_out, Hostname:""}
  }

  // perform reduce
  reduce_out := strings.Join([]string{file, "reduce"}, "-")
  reduce_args := DoJobArgs{Operation:ReduceByKeyJob, InputIDs:reduce_in, OutputID:reduce_out, Function:"SumInt"}
  var reduce_reply DoJobReply
  for !mr.AssignJob(w, &reduce_args, &reduce_reply) { // if worker fails, roughly, wait for another available worker
    w = get_first_worker(mr)
  }

  // get result
  get_args := DoJobArgs{Operation:GetSplit, InputID:reduce_out}
  var get_reply DoJobReply
  for !mr.AssignJob(w, &get_args, &get_reply) { // if worker fails, roughly, wait for another available worker
    w = get_first_worker(mr)
  }

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

func TestMasterGetSplit(t *testing.T) {
  fmt.Printf("Test: Master Get Split...\n")
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

  file := "hdfs://vision24.csail.mit.edu:54310/user/kmean_data.txt"

  // read from HDFS
  read_out := "split0"
  read_args := DoJobArgs{Operation:ReadHDFSSplit, HDFSFile:file, HDFSSplitID:0, OutputID:read_out}
  var read_reply DoJobReply
  mr.AssignJob(w, &read_args, &read_reply)

  get_args := DoJobArgs{Operation:GetSplit, InputID:read_out}
  var get_reply DoJobReply
  mr.AssignJob(w, &get_args, &get_reply)

  mr.Shutdown()
  // TODO check
  //check(t, mr.file)
  //checkWorker(t, mr.stats)
  //cleanup(mr)
  //fmt.Printf("  ... File Read Passed\n")
}

func TestMasterMRCharCountStruct(t *testing.T) {
  fmt.Printf("Test: Master MapReduce Char Count with Custom Struct...\n")
  mr := make_master()
  file := "hdfs://vision24.csail.mit.edu:54310/user/kmean_data.txt"
  nsplits := hadoop.GetSplitInfo(file).Len()
  w := get_first_worker(mr)
  reduce_in := make([]Split, nsplits)
  for i := 0; i < nsplits; i++ {
    // read from HDFS
    read_out := strings.Join([]string{file, "read", strconv.Itoa(i)}, "-")
    read_args := DoJobArgs{Operation:ReadHDFSSplit, HDFSFile:file, HDFSSplitID:i, OutputID:read_out}
    var read_reply DoJobReply
    for !mr.AssignJob(w, &read_args, &read_reply) { // if worker fails, roughly, wait for another available worker
      w = get_first_worker(mr)
    }
    // perform map
    map_out := strings.Join([]string{file, "map", strconv.Itoa(i)}, "-")
    map_args := DoJobArgs{Operation:MapJob, InputID:read_out, OutputID:map_out, Function:"CharCountStruct"}
    var map_reply DoJobReply
    for !mr.AssignJob(w, &map_args, &map_reply) { // if worker fails, roughly, wait for another available worker
      w = get_first_worker(mr)
    }
    reduce_in[i] = Split{SplitID:map_out, Hostname:""}
  }
  // perform reduce
  reduce_out := strings.Join([]string{file, "reduce"}, "-")
  reduce_args := DoJobArgs{Operation:ReduceByKeyJob, InputIDs:reduce_in, OutputID:reduce_out, Function:"SumIntStruct"}
  var reduce_reply DoJobReply
  for !mr.AssignJob(w, &reduce_args, &reduce_reply) { // if worker fails, roughly, wait for another available worker
    w = get_first_worker(mr)
  }

  // get result
  get_args := DoJobArgs{Operation:GetSplit, InputID:reduce_out}
  var get_reply DoJobReply
  for !mr.AssignJob(w, &get_args, &get_reply) { // if worker fails, roughly, wait for another available worker
    w = get_first_worker(mr)
  }


  mr.Shutdown()
  // TODO check
  //check(t, mr.file)
  //checkWorker(t, mr.stats)
  //cleanup(mr)
  //fmt.Printf("  ... File Read Passed\n")
}

