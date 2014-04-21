package main

import "fmt"
import "log"
import "os/exec"
import "bufio"
import "container/list"
import "strings"
import "unicode"
import "strconv"

//TODO: handle file read error / file not exists
func getSplitScanner(fileURI string, splitInd int) (* bufio.Scanner) {
	cmd := exec.Command("java", "HDFSSplitReaderStable", fileURI, strconv.Itoa(splitInd))
	stdout, err := cmd.StdoutPipe()
  
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
    scanner := bufio.NewScanner(stdout)
    return scanner
}


//TODO: handle file read error / file not exists
func getSplitInfo(fileURI string) (*list.List) {
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

func main(){

    fileURI := "hdfs://127.0.0.1:54310/user/hduser/testSplitRead.txt";
    
	s := getSplitInfo(fileURI)
	nsplit := s.Len();
	
	fmt.Printf("number of split: %d\n", nsplit);
	for it := s.Front(); it != nil; it=it.Next() {
	    slist := it.Value.([]string)
	    for j:=0; j<len(slist); j++ {
	        fmt.Printf(slist[j]);
	    }
	    fmt.Println();
	}
    
 
    scanner := getSplitScanner(fileURI, 0); // get the scanner of split 0
    
    for scanner.Scan() {
		fmt.Println(scanner.Text()) // read one line of data in split
	}
	
}
