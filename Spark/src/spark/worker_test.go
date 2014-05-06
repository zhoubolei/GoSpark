package spark

import (
  "testing"
  "fmt"
  "log"
  "os"
  "bufio"
  "time"
  "math/rand"
  "strings"
  "strconv"
  "encoding/gob"
)

// custom types
func register_types() {
  gob.Register(CenterCounter{})
  gob.Register([]Vector{})
  gob.Register(MyStruct{})
}


func (u *UserFunc) LineCount(line KeyValue) interface{} {
  return KeyValue{Key:"x", Value:1}
}

func (u *UserFunc) SumInt(a KeyValue, b KeyValue) interface{} {
  return a.Value.(int) + b.Value.(int)
}


type MyStruct struct {
  N int
}

func (u *UserFunc) CharCountStruct(line KeyValue) interface{} {
  cnt := len(line.Value.(string))
  return KeyValue{Key:"x", Value:MyStruct{N:cnt}}
}

func (u *UserFunc) SumIntStruct(a KeyValue, b KeyValue) interface{} {
  return MyStruct{N:a.Value.(MyStruct).N + b.Value.(MyStruct).N}
}


func make_worker(unrel bool) *Worker{
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

  // worker ip
  f2, err2 := os.Open("/etc/hostname")
  if err2 != nil {
    log.Fatal(err2)
  }
  s2 := bufio.NewScanner(f2)
  s2.Scan()
  my_ip := s2.Text()

  // worker port
  rand.Seed(time.Now().UTC().UnixNano())
  port := 5000 + rand.Intn(1000)
  my_port := strings.Join([]string{":", strconv.Itoa(port)}, "")
  fmt.Printf("worker ip %s port %s\n", my_ip, my_port)

  return MakeWorker(master_ip, master_port, my_ip, my_port, unrel)
}

func TestBasicWorker(t *testing.T) {
  fmt.Printf("Test: Basic Worker...\n")
  wk := make_worker(false)
  <-wk.DoneChannel
}

func TestWorkerRPCUnrel(t *testing.T) {
  fmt.Printf("Test: Worker with Unreliable RPC...\n")
  wk := make_worker(true)
  <-wk.DoneChannel
}

func TestWorkerSuperUnrel(t *testing.T) {
  fmt.Printf("Test: Worker with Unreliable RPC and Crash Reboot ...\n")
  wk := make_worker(true)
  for wk.alive {
    // up for a random period of time
    duration := rand.Intn(100)
    for i := 0; i < duration; i++ {
      time.Sleep(time.Second)
      if !wk.alive {
        break
      }
    }
    // crash and reboot
    if wk.alive {
      wk.kill()
      wk = make_worker(true)
    }
  }
}

