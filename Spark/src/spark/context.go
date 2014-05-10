package spark

import (
  //"encoding/gob"
  "time"
  "hadoop"
  "log"
  "fmt"
)

type Context struct {
    jobName    string
    scheduler  *Scheduler
    initialzed bool
    started    bool
    startTime  time.Time
}

type UserFunc struct {}

type KeyValue struct {
    Key   interface{}
    Value interface{}
}

type Pair struct {
    First interface{}
    Second interface{}
}

func (kv *KeyValue) String() string {
  return fmt.Sprintf("%v:%v", kv.Key, kv.Value)
}

func NewContext(jobName string) *Context{
  c := Context{jobName: jobName}
  c.scheduler = NewScheduler()
  c.startTime = time.Now()
  log.Printf("Context [%s] is started.", c.jobName)
  //gob.Register(Vector{})
  return &c
}

// Initialize HDFS RDD 
func (c *Context) TextFile(fileURI string) *RDD {
  s := hadoop.GetSplitInfoSlice(fileURI)
  
  rdd := makeRDD(c, len(s), Narrow, RangePartition, HDFSFile, fileURI, "", nil, nil)
  rdd.hadoopSplitInfo = s;
  return rdd
}

func (c *Context) Stop(){
  // clean up memory in workers
  
}
