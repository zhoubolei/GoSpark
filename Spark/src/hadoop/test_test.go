package hadoop

import (
    "testing"
    "fmt"
    "log"
)

const Debug=1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

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
    
  scanner, err := GetSplitScanner(fileURI, 0); // get the scanner of split 0
  
  if err == nil {
	  for scanner.Scan() {
			fmt.Println(scanner.Text()) // read one line of data in split
		}
	} else {
	  fmt.Println(err)
	}
}

func TestGetInfoSlice(t *testing.T) {
  //fileURI := "hdfs://127.0.0.1:54310/user/kmean_data.txt";
  fileURI := "hdfs://vision24.csail.mit.edu:54310/user/featureSUN397.csv";
	s := GetSplitInfoSlice(fileURI)
	nsplit := len(s);
	DPrintf("nsplit = %v\n", nsplit)
	fmt.Printf("This file has %d splits\n", nsplit);
	for i := 0; i < nsplit; i++ {
	    slist := s[i]
	    for j:=0; j<len(slist); j++ {
	        fmt.Printf("%v ", slist[j]);
	    }
	    fmt.Println();
	}
    
}
