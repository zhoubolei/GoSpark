package spark

import (
  "time"
  "hadoop"
  "encoding/gob"
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

type UserData struct {
    Data interface{}
}

type KeyValue struct {
    Key   UserData
    Value UserData
}

func (kv *KeyValue) String() string {
  return fmt.Sprintf("%v:%v", kv.Key.Data, kv.Value.Data)
}

func NewContext(jobName string) *Context{
  c := Context{jobName: jobName}
  c.scheduler = NewScheduler()
  c.startTime = time.Now()
  log.Printf("Context [%s] is started.", c.jobName)
  gob.Register(new(KeyValue))
  return &c
}

// Initialize HDFS RDD 
func (c *Context) TextFile(fileURI string) *RDD {
  s := hadoop.GetSplitInfo(fileURI)
  
  rdd := makeRDD(c, s.Len(), Narrow, RangePartition, HDFSFile, fileURI, "", nil, nil)
  
  return rdd
}

func (c *Context) Stop(){
  // clean up memory in workers
  
}
