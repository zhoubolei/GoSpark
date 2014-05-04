package hadoop

import (
    "testing"
    "fmt"
)

func TestBasicRead(t *testing.T) {
//    fileURI := "hdfs://127.0.0.1:54310/user/hduser/testSplitRead.txt";
//  fileURI := "hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv";
  fileURI := "hdfs://127.0.0.1:54310/user/kmean_data.txt";

	s := GetSplitInfo(fileURI)
	nsplit := s.Len();
	
	fmt.Printf("This file has %d splits\n", nsplit);
	for it := s.Front(); it != nil; it=it.Next() {
	    slist := it.Value.([]string)
	    for j:=0; j<len(slist); j++ {
	        fmt.Printf("%v ", slist[j]);
	    }
	    fmt.Println();
	}
    
    scanner, _ := GetSplitScanner(fileURI, 0); // get the scanner of split 0
    
    for scanner.Scan() {
		fmt.Println(scanner.Text()) // read one line of data in split
	}
}

