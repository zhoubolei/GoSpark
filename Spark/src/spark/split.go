package spark

type Split struct {
  SplitID  string
  Hostname string
}

func makeSplit() *Split{
  s := Split{}
  s.SplitID = nrand()
  return &s
}
