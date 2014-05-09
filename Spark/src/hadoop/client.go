package hadoop

import "log"
import "os/exec"
import "bufio"
import "container/list"
import "strings"
import "unicode"
import "strconv"
import "fmt"

func GetSplitScanner(fileURI string, splitInd int) (* bufio.Scanner, error) {
	cmd := exec.Command("java", "HDFSSplitReaderStable", fileURI, strconv.Itoa(splitInd))
	stdout, err := cmd.StdoutPipe()
  
	if err != nil {
		log.Println(err)
    return nil, err
	}
	if err := cmd.Start(); err != nil {
		log.Println(err)
    return nil, err
	}
  scanner := bufio.NewScanner(stdout)
  if scanner.Scan() {
    if scanner.Text() != "1" {
      return nil, fmt.Errorf("HDFS read fileURI:%v splitInd:%v error", fileURI, splitInd)
    }
  } else {
    return nil, fmt.Errorf("HDFS read fileURI:%v splitInd:%v error", fileURI, splitInd)
  }
  return scanner, nil
}


//TODO: handle file read error / file not exists
func GetSplitInfo(fileURI string) (*list.List) {
	cmd := exec.Command("java", "HDFSGetSplitInfo", fileURI)
	stdout, err := cmd.StdoutPipe()
  
  
  
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
  scanner := bufio.NewScanner(stdout)
    
  l := list.New()
  for i:=0; scanner.Scan(); i++ {
		strlist := scanner.Text() 
		var slist []string = strings.FieldsFunc(strlist, unicode.IsSpace); // the server list by splitting value
		l.PushBack(slist)
	}
	return l
}

//TODO: handle file read error / file not exists
func GetSplitInfoSlice(fileURI string) [][]string {
	cmd := exec.Command("java", "HDFSGetSplitInfo", fileURI)
	stdout, err := cmd.StdoutPipe()
  
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
  scanner := bufio.NewScanner(stdout)
    
  l := make([][]string, 0)
  for i:=0; scanner.Scan(); i++ {
		strlist := scanner.Text() 
		var slist []string = strings.FieldsFunc(strlist, unicode.IsSpace); // the server list by splitting value
		l = append(l, slist)
	}
	return l
}

