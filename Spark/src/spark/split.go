package spark

type Split struct {
  splitID  string
  hostname string
}

func makeSplit() *Split{
  s := Split{}
  s.splitID = nrand()
  return &s
}