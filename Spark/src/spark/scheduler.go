package spark
 
import "fmt"
import "os"
import "bufio"
import "log"
import "math/rand"
import "strings"
import "time"

type Scheduler struct {
    master *Master
} 

type Node struct{
  rdd       *RDD
  processed bool
} 

type Dag struct{
  root *Node
  nodes []*Node
  edge map[*Node] []*Node
} 

func buildDagRun(rdd *RDD, dag *Dag, parent *Node){
  
  if rdd.isTarget {
    targetNode := &Node{}
    targetNode.rdd = rdd
    dag.root = targetNode
    parent = targetNode
    dag.nodes = append(dag.nodes, targetNode)
  }
    
  if(rdd.dependType == Wide) {
    children := []*Node{}
    if(rdd.prevRDD1 != nil) {
      childNode := &Node{rdd: rdd.prevRDD1}
      children = append(children, childNode)
      buildDagRun(rdd.prevRDD1, dag, childNode)
      dag.nodes = append(dag.nodes, childNode)
    }
    if(rdd.prevRDD2 != nil) {
      childNode := &Node{rdd: rdd.prevRDD2}
      children = append(children, childNode)
      buildDagRun(rdd.prevRDD2, dag, childNode)
      dag.nodes = append(dag.nodes, childNode)
    }
    dag.edge[parent] = children
  } else {
	  if(rdd.prevRDD1 != nil) {
	    buildDagRun(rdd.prevRDD1, dag, parent)
	  }
	  if(rdd.prevRDD2 != nil) {
	    buildDagRun(rdd.prevRDD2, dag, parent)
	  }
  }
}

func makeDagFromRdd(rdd *RDD) *Dag{
  dag := &Dag{}
  dag.nodes = []*Node{}
  dag.edge = make(map[*Node] []*Node)
  buildDagRun(rdd, dag, nil)
  return dag
}

func topSort(dag *Dag) []*RDD{
  length := len(dag.edge)
  list := make([]*RDD, length+1)
  
  for i:=0; i< length; i++ {
    // find a node that does not depend on unprocessed node or is a leaf
    for _, node := range dag.nodes {
      // if no children
      if _, hasval:= dag.edge[node]; !hasval {
        list[i] = node.rdd
        node.processed = true
        break
      }
      // if children are not all processed then find next one
      children := dag.edge[node];
      for _, childnode := range children {
        if childnode.processed == false {
          continue
        }
      }
    }
  }
  list[length] = dag.root.rdd
  
  return list
}

// turn addressHDFS like vision24.csail.mit.edu to vision24:portname
func (d *Scheduler) findServerAddress(addressHDFS string) string {
  DPrintf("addressHDFS: %v\n", addressHDFS)
  hostname := strings.FieldsFunc(addressHDFS, func(c rune) bool { return c == '.' })[0]
  DPrintf("hostname: %v\n", hostname)
  m := d.master.WorkersAvailable()
  for hostnameWithPort,_ := range m {
    DPrintf("hostnameWithPort: %v\n", hostnameWithPort)
    if(strings.HasPrefix(hostnameWithPort, hostname)) {
      DPrintf("hostnameWithPort: %v hostname: %v\n", hostnameWithPort, hostname)
      return hostnameWithPort
    }
  }
  DPrintf("return ''\n")
  return ""
}

// turn addressesHDFS 
func (d *Scheduler) findServerAddresses(addressesHDFS []string) []string {
  hostnamesWithPort := []string {}
  for _, addressHDFS := range addressesHDFS {
    if hostnameWithPort := d.findServerAddress(addressHDFS); hostnameWithPort != "" {
      hostnamesWithPort := append(hostnamesWithPort, hostnameWithPort)
    }
  }
  return hostnamesWithPort
}


func randomWorkerFromMap(wlist map[string] WorkerInfo) string {
	randInd := rand.Int() % len(wlist)
	for k := range wlist {
	  
	  if randInd == 0 {
      return k
    }
    randInd--;
  }
  return ""
}

const (
  RetryInterval          = 100 * time.Millisecond
  RetryMax               = 10
}
// SpInd here is 0~nSplit-1 of rdd, not the 64bit id
func (d *Scheduler) runThisSplit(rdd *RDD, SpInd int) error {
  DPrintf("Scheduler.runThisSplit name:%v op: %v SpInd:%d start",  rdd.name, rdd.operationType, SpInd)
  defer DPrintf("Scheduler.runThisSplit name:%v op: %v SpInd:%d end",  rdd.name, rdd.operationType, SpInd )
  
  switch rdd.operationType {
  case HDFSFile:
	  sOut := rdd.splits[SpInd]
    if sOut.Hostname != "" && splitExistAt(sOut.Hostname, sOut.SplitID) { 
      return nil
    }
	  reply := DoJobReply{}
	  args := DoJobArgs{Operation: ReadHDFSSplit, OutputID: sOut.SplitID, HDFSSplitID: SpInd, HDFSFile: rdd.filePath};

	  DPrintf("len(sinfo) = %d\n", len(rdd.hadoopSplitInfo))
	  serverList := rdd.hadoopSplitInfo[SpInd]  // server the hold this HDFS Split
	    
	  addressOfWorkersInMaster := d.findServerAddresses(serverList)
	  
	  // Call master with preferred list, but not restricted to those because HDFS can read remotely though slower
	  nRetry := 0
	  for ok, successWorker := d.master.AssignJob(addressOfWorkersInMaster, false, &args, &reply); !ok {
	    if ++nRetry > RetryMax {
	      log.Fatalf("Scheduler.runThisSplit HDFSFile fail after %d retries, name:%v SpInd:%d worker:%v, retry after %v", RetryMax, rdd.name, SpInd, addressWorkerInMaster, RetryInterval)
	      break
	    }
			log.Printf("Scheduler.runThisSplit HDFSFile fail, name:%v SpInd:%d worker:%v, retry %d after %v",  rdd.name, SpInd, addressWorkerInMaster, nRetry, RetryInterval) 
	    time.Sleep(RetryInterval)
	    rdd.splits[SpInd].Hostname = successWorker
	  }
	  
  case Map:
	  sIn := rdd.prevRDD1.splits[SpInd]
	  sOut := rdd.splits[SpInd]
	  reply := DoJobReply{}
	  args := DoJobArgs{Operation: MapJob, InputID: sIn.SplitID, OutputID: sOut.SplitID, Function: rdd.fnName, Data: rdd.fnData, HDFSSplitID: SpInd};  /*SpInd is put in HDFSSplitID for profiling purpose */
	  ok, _ := d.master.AssignJob([]string{sIn.Hostname}, true, &args, &reply)
	  if(!ok) { 
	    log.Printf("Scheduler.runThisSplit Map not ok, name:%v SpInd:%d worker:%v",  rdd.name, SpInd, sIn.Hostname)
	    sOut.Hostname = "" 
	    return fmt.Errorf("Bad mapping")
	  } else {
	    sOut.Hostname = sIn.Hostname
	  }
	  
  case ReduceByKey:
    // shuffleSplits should be moved to rdd from prev rdd 
	  sOut := rdd.splits[SpInd]
	  
    var ss [][]*Split// shuffleSplits
    nSpl := rdd.prevRDD1.length
    nRed := rdd.length
    
    rdd.prevRDD1.shuffleMu.Lock()
    // If shuffle files not correspond to the output
    if(rdd.prevRDD1.shuffleN != nRed) {
      rdd.prevRDD1.shuffleN = nRed
      ss = make([][]*Split, nSpl)
      for i:=0; i<nSpl; i++ {
        ss[i] = make([]*Split, nRed)
        for j:=0; j<nRed; j++ {
          ss[i][j] = makeSplit()
          ss[i][j].Hostname = rdd.prevRDD1.splits[i].Hostname
        }
      }
      rdd.prevRDD1.shuffleSplits = ss  // may want to delete old shuffle splits
    } else {
      ss = rdd.prevRDD1.shuffleSplits
    }
    
    
      
    y := make(Yielder)
    // do hashPart on each input split
    for i:=0; i<nSpl; i++ {
		  go func(i int, nRed int){
			  OutputIDs := make([]Split, nRed)
			  for j:=0; j<nRed; j++ { OutputIDs[j] = *(ss[i][j]) }
			  args := DoJobArgs{Operation: HashPartJob, InputID: rdd.prevRDD1.splits[i].SplitID, OutputIDs: OutputIDs};
			
			  reply := DoJobReply{}
			  
        if ok, _ := d.master.AssignJob([]string{rdd.prevRDD1.splits[i].Hostname}, true, &args, &reply); !ok  { 
          log.Printf("Scheduler.runThisSplit HashPartJob fail, name:%v SpInd:%d worker:%v, rerun the Split in prevRDD",  rdd.name, SpInd, rdd.prevRDD1.splits[i].Hostname) 
			    y <- i
        }
        
        // Now the hash splits of this split is done, update the split  
			  for j:=0; j<nRed; j++ { 
          (*(ss[i][j])).Hostname = rdd.prevRDD1.splits[i].Hostname			      
			  }
	      
			  y <- nil
			} (i, nRed)
    }
    // aggregate all missing splits
    misSplitInd := []string{} 
    for i:=0; i<nSpl; i++ {
      output := <- y
      switch p := p.(type) {
      case int:
          misSplitInd = append(misSplitInd, strconv.Itoa(p))
      }
    }
    if len(misSplitInd) > 0 {
      rdd.prevRDD1.shuffleMu.Unlock()
      return fmt.Errorf("ReduceByKey %s", strings.Join(misSplitInd, " "))
    }
    rdd.prevRDD1.shuffleMu.Unlock()
    
    
    // now we can do reduce
		InputIDs := make([]Split, nSpl)
    reply := DoJobReply{}
		for i:=0; i<nSpl; i++ { InputIDs[i] = *(ss[i][SpInd]) }
		
    sOut.Hostname = randomWorkerFromMap(d.master.WorkersAvailable()) // get one from some free worker
    args := DoJobArgs{Operation: ReduceByKeyJob, InputIDs: InputIDs, OutputID: sOut.SplitID, Function: rdd.fnName, Data: rdd.fnData, HDFSSplitID: SpInd};
       
	  for ok, _ := d.master.AssignJob([]string{}, false, &args, &reply); !ok {    // randomly pick a worker 
	    log.Printf("Scheduler.runThisSplit ReduceByKey not ok, name:%v SpInd:%d worker:%v",  rdd.name, SpInd, sOut.Hostname)  
      time.Sleep(RetryInterval)
	  }
	  
	  if !reply.OK {
	    // rerun the missing splits, can be done in parallel
	    for _, misSplitID := range reply.NeedSplits {
	      ind := findMisSplitIndFromMap(ss, misSplitID)
        time.Sleep(RetryInterval)
	      d.runSplit( rdd.prevRDD1, i )
	    }
      rdd.prevRDD1.shuffleMu.Lock()
	  }
	  
	 
  }
  return nil
}

func findMisSplitIndFromMap(sp [][]*Split, splitId string) int {
  for i, _ := range sp {
    for j, _ := range sp[i] {
      if *(sp[i][j]).SplitID == splitId {
        return i
      }
    }
  }
  return -1 // fatal
}

// gaurantees the split is done by recursively run the previous split in this stage given previous stage is done
func (d *Scheduler) runSplit(rdd *RDD, SpInd int) Yielder {
  DPrintf("Scheduler.runSplit name:%v op: %v SpInd:%d start",  rdd.name, rdd.operationType, SpInd)
  y := make(Yielder)
  go func(){
    defer DPrintf("Scheduler.runSplit name:%v op: %v SpInd:%d end",  rdd.name, rdd.operationType, SpInd )
    var cy1, cy2 Yielder
RERUN:
    if rdd.dependType != Wide && rdd.prevRDD1 != nil {
      cy1 = d.runSplit(rdd.prevRDD1, SpInd)
    } 
    if rdd.dependType != Wide && rdd.prevRDD2 != nil {
      cy2 = d.runSplit(rdd.prevRDD2, SpInd)
    } 
    if(cy1 != nil){ 
      if Err := <- cy1; Err != nil {
        y <- Err
        close(y)
        return
      } 
    }
    if(cy2 != nil){ 
      Err := <- cy2; 
      if Err := <- cy1; Err != nil {
        y <- Err
        close(y)
        return
      }   
    }
    
    err := d.runThisSplit(rdd, SpInd)
    if err != nil {
      if rdd.dependType == Wide {
        y <- fmt.Errorf("")
        close(y)
        return
      } else {
        goto RERUN
      }        
    }
    
    y <- nil
    close(y)
  } ()
  return y
}

func (d *Scheduler) runRDDInStage(rdd* RDD) {
  ys := []Yielder{}
  for i:=0; i<rdd.length; i++ {
    DPrintf("Scheduler.runRDDInStage name:%v op: %v SpInd:%d start",  rdd.name, rdd.operationType, i)
    ys = append(ys, d.runSplit(rdd, i))
  }
  // wait for each split to complete, 
  // TODO handle failure here
  for i:=0; i<rdd.length; i++ {
    <- ys[i]
  }
}

// Recursively Compute RDD by stages and not retrieve data to master 
func (d *Scheduler) computeRDDByStage(rdd* RDD) {
  rdd.isTarget = true;
  // 1. build DAG graph for stages
  dag := makeDagFromRdd(rdd)
  DPrintf("Dag: %v", dag)
  // 2. topological sort DAG
  sortedList := topSort(dag)
  DPrintf("SortedList: ")
  for i:=0; i<len(sortedList); i++ {
    DPrintf("  %2d rdd: %v op: %v\n", i+1, (*(sortedList[i])).name, (*(sortedList[i])).operationType)
  }
  // 3. Run each stage according to sorted order
  for i:=0; i<len(sortedList); i++ {
    d.runRDDInStage(sortedList[i])
  }
  rdd.isTarget = false;
}

// This function is called when user triggers an action
// The reduce function rn is applied to all data "in a split"
func (d *Scheduler) computeRDD(rdd* RDD, operationType string, fn string) []interface{} {
   
  nOutputSplit := rdd.length
  
  d.computeRDDByStage(rdd)
  
  switch (operationType) {
  case "Collect":
    // simply collect without applying reduce function
    ret := []interface{} {}
	  for i:=0; i<nOutputSplit; i++ {
	    s := rdd.splits[i]
	    reply := DoJobReply{}
	    args := DoJobArgs{Operation: GetSplit, InputID: s.SplitID};
	         
	    ok, _ := d.master.AssignJob([]string{s.Hostname}, true, &args, &reply) // shenjiasi: need to change these args
	    if !ok {
        log.Printf("In Scheduler.computeRDD, Op=%v, Split=%v, => rerun\n",operationType, s)
      }
      ret = append(ret, reply.Lines)  // append one slice to another : add ...
	  }
    return ret 
  case "Count":
  // TODO:
  
  case "Reduce":
    ret := []interface{} {}
	  for i:=0; i<nOutputSplit; i++ {
	    s := rdd.splits[i]
	    reply := DoJobReply{}
	    args := DoJobArgs{Operation: ReduceJob, InputIDs: []Split{*s}, Function: fn, HDFSSplitID: i}
	         
	    ok, _ := d.master.AssignJob([]string{s.Hostname}, true, &args, &reply) // shenjiasi: need to change these args
	    if !ok {
        log.Printf("In Scheduler.computeRDD, Op=%v, Split=%v, => rerun\n",operationType, s)
      } else {
        DPrintf("In Scheduler.computeRDD, reply.Result=%v\n",reply.Result)
      }
      DPrintf("reply.Result=%v", reply.Result)
      ret = append(ret, reply.Result)  // append one slice to another : add ...
	  }
    
    return ret 
  }
    
    
    
  return nil
}

func NewScheduler() *Scheduler {
    scheduler := Scheduler{} 
	  f, err := os.Open("config.txt")
	  if err != nil {
	    log.Fatal(err)
	  }
	  s := bufio.NewScanner(f)
	  s.Scan()
	  master_ip := s.Text()
	  s.Scan()
	  master_port := s.Text()
	  fmt.Printf("In NewScheduler: master ip %s port %s\n", master_ip, master_port)
  
    address := master_ip 
    port := master_port
    scheduler.master = MakeMaster(address, port)

    return &scheduler
}
