package spark
 
import "fmt"
import "os"
import "bufio"
import "log"
import "hadoop"
import "math/rand"
import "strings"

type Scheduler struct {
    master *Master
} 

func Call(hostname string, fnName string, args *DoJobArgs, reply *DoJobReply) bool {
  //return call(hostname, fnName, args, reply)
  DPrintf("Call(%v, %v, %v)\n", hostname, fnName, args)
  return true
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
  list[length+1] = dag.root.rdd
  
  return list
}

// turn addressHDFS like vision24.csail.mit.edu to vision24:portname
func (d *Scheduler) findServerAddress(addressHDFS string) string {
  hostname := strings.FieldsFunc(addressHDFS, func(c rune) bool { return c == '.' })[0]
  
  m := d.master.WorkersAvailable()
  for hostnameWithPort,_ := range m {
    if(strings.HasPrefix(hostnameWithPort, hostname)) {
      return hostnameWithPort
    }
  }
  return ""
}

func (d *Scheduler) runThisSplit(rdd *RDD, SpInd int) error {
  // if ran than check if the result exists, if exists don't run again;
  switch rdd.operationType {
  case HDFSFile:
	  sOut := rdd.splits[SpInd]
	  reply := DoJobReply{}
	  args := DoJobArgs{Operation: ReadHDFSSplit, OutputID: sOut.SplitID, HDFSSplitID: SpInd, HDFSFile: rdd.filePath};
	  
	  sinfo := hadoop.GetSplitInfoSlice(HDFSFile)
	  serverList := sinfo[SpInd]
	  sid   := rand.Int() % len(serverList)  // randomly pick one
	  addressHDFS := serverList[sid]
	  addressWorkerInMaster := d.findServerAddress(addressHDFS)
	  rdd.splits[SpInd].Hostname = addressWorkerInMaster
	  
	  ok := Call(addressWorkerInMaster, "Worker.DoJob", &args, &reply)
	  if(!ok) { log.Printf("Scheduler.runThisSplit Map not ok") }
  //case MapWithData:
  
  case Map:
	  sIn := rdd.prevRDD1.splits[SpInd]
	  sOut := rdd.splits[SpInd]
	  reply := DoJobReply{}
	  args := DoJobArgs{Operation: MapJob, InputID: sIn.SplitID, OutputID: sOut.SplitID};
	  ok := call(sIn.Hostname, "Worker.DoJob", &args, &reply)
	  if(!ok) { log.Printf("Scheduler.runThisSplit Map not ok") }
	  
	  
  case ReduceByKey:
    // shuffleSplits should be moved to rdd from prev rdd 
	  sOut := rdd.splits[SpInd]
	  
    var ss [][]*Split// shuffleSplits
    nSpl := rdd.prevRDD1.length
    nRed := rdd.length
    // If shuffle files not correspond to the output
    
    rdd.prevRDD1.shuffleMu.Lock()
    if(rdd.prevRDD1.shuffleN != nRed) {
      rdd.prevRDD1.shuffleN = nRed
      ss := make([][]*Split, nSpl)
      for i:=0; i<nSpl; i++ {
        ss[i] = make([]*Split, nRed)
        for j:=0; j<nRed; j++ {
          ss[i][j] = makeSplit()
          ss[i][j].Hostname = rdd.prevRDD1.splits[i].Hostname
        }
      }
      
      rdd.prevRDD1.shuffleSplits = ss  // may want to delete old shuffle splits
      
      y := make(Yielder)
      // do hashPart on each input split
      for i:=0; i<nSpl; i++ {
			  OutputIDs := make([]Split, nRed)
			  for j:=0; j<nRed; j++ { OutputIDs[j] = *ss[i][j] }
			  args := DoJobArgs{Operation: HashPartJob, InputID: rdd.prevRDD1.splits[i].SplitID, OutputIDs: OutputIDs};
			  go func(args DoJobArgs){
			    reply := DoJobReply{}
			    ok := call(rdd.prevRDD1.splits[i].Hostname, "Worker.DoJob", &args, &reply)
	        if(!ok) { log.Printf("Scheduler.runThisSplit HashPartJob not ok") }
			    y <- 1
			  } (args)
      }
      for i:=0; i<nSpl; i++ {
        <- y
      }
    } else {
      ss = rdd.prevRDD1.shuffleSplits
    }
    rdd.prevRDD1.shuffleMu.Unlock()
    
    
    // now we can do reduce
		InputIDs := make([]Split, nSpl)
    reply := DoJobReply{}
		for i:=0; i<nSpl; i++ { InputIDs[i] = *ss[i][SpInd] }
    // sOut.hostname = get one from some free worker 
    args := DoJobArgs{Operation: ReduceByKey, InputIDs: InputIDs, OutputID: sOut.SplitID};
    ok := call(sOut.Hostname, "Worker.DoJob", &args, &reply)
	  if(!ok) { log.Printf("Scheduler.runThisSplit ReduceByKey not ok") }
	  
	    
  }
  return nil
}

func (d *Scheduler) runSplit(rdd *RDD, SpInd int) Yielder {
  y := make(Yielder)
  go func(){
    var cy1, cy2 Yielder
    if rdd.dependType != Wide && rdd.prevRDD1 != nil {
      cy1 = d.runSplit(rdd.prevRDD1, SpInd)
    } 
    if rdd.dependType != Wide && rdd.prevRDD2 != nil {
      cy2 = d.runSplit(rdd.prevRDD2, SpInd)
    } 
    if(cy1 != nil){ <- cy1 }
    if(cy2 != nil){ <- cy2 }
    /*err := */d.runThisSplit(rdd, SpInd)
    y <- 1
    close(y)
  } ()
  return y
}

func (d *Scheduler) runRDDInStage(rdd* RDD) {
  for i:=0; i<rdd.length; i++ {
    d.runSplit(rdd, i);
  }
}

// Recursively Compute RDD by stages and not retrieve data to master 
func (d *Scheduler) computeRDDByStage(rdd* RDD) {
  // 1. build DAG graph for stages
  dag := makeDagFromRdd(rdd)
  // 2. topological sort DAG
  sortedList := topSort(dag)
  // 3. Run each stage according to sorted order
  for i:=0; i<len(sortedList); i++ {
    d.runRDDInStage(sortedList[i])
  }
}

// This function is called when user triggers an action
// The reduce function rn is applied to all data "in a split"
func (d *Scheduler) computeRDD(rdd* RDD, operationType string, fn string) []interface{} {
   
  nOutputSplit := rdd.length
  
  d.computeRDDByStage(rdd)
  
  switch (operationType) {
  case "Collect":
    // simply collect without applying reduce function
  
	  for i:=0; i<nOutputSplit; i++ {
	    s := rdd.splits[i]
	    reply := DoJobReply{}
	    args := DoJobArgs{Operation: "GetSplit", InputID: s.SplitID};
	         
	    ok := call(s.Hostname, "Worker.DoJob", &args, &reply)
	    if !ok {
        log.Printf("In Scheduler.computeRDD, Split%d, => rerun\n")
      }
	  }
    
  case "Count":
  // TODO:
  
  case "Reduce":
  // TODO:

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
