package hadoop

import (
    "testing"
    "fmt"
)

func TestBasicRead(t *testing.T) {
    fileURI := "hdfs://127.0.0.1:54310/user/hduser/testSplitRead.txt";
    
	s := getSplitInfo(fileURI)
	nsplit := s.Len();
	
	fmt.Printf("This file has %d splits\n", nsplit);
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

