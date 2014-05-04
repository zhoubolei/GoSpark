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
)

func (u *UserFunc) LineCount(line UserData, data UserData) UserData {
  return UserData{Data:KeyValue{Key:UserData{Data:"x"}, Value:UserData{Data:1}}}
}

func (u *UserFunc) SumInt(a UserData, b UserData, data UserData) UserData {
  sum := a.Data.(int) + b.Data.(int)
  return UserData{Data:sum}
}

func TestBasicWorker(t *testing.T) {
  fmt.Printf("Test: Basic Worker...\n")

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

  RunWorker(master_ip, master_port, my_ip, my_port, -1)
}
