package spark 

import "fmt"
import "os"
import "bufio"
import "log"
import "math/rand"

type Scheduler struct {
    master *Master
}

func Call(hostname string, args *DoJobArgs, reply *DoJobReply) error{
  return nil
}

func (d *Scheduler) computeRDD(rdd* RDD, operationType string, fn string) []interface{} {
    // This function is called when user triggers an action or before an RDD go into a wide dependency process like reduceByKey
    // The reduce function rn collect all data "in a split" from an RDD with a yielder and do some reduce process
    // []Yielder is an array of splits' product after applying rn on each split
  nOutputSplit := rdd.length
    
  for i:=0; i<nOutputSplit; i++ {
    s := rdd.splits[i]
    reply := DoJobReply{}
    switch operationType {
    case "Collect":
      args := DoJobArgs{Operation: "GetSplit", InputID: s.splitID};
    case "Reduce":
      args := DoJobArgs{Operation: "Reduce", InputID: s.splitID};
    }
    rnd := rand.Int() % len(s.hostname)
    for j:=0; j<len(s.hostname); j++ {
      k := (rnd + j) % len(s.hostname)
      
      ok := call(s.hostname[k], "Worker.DoJob", &args, &reply)
      if ok {
        break
      }
    }
    
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
    scheduler.master = MakeMaster("", "", address, port)   
    
    return &scheduler
}
