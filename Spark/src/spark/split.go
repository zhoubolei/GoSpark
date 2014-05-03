package spark

type Split struct {
  splitID string
  hostname []string
}

func makeSplit() *Split{
  s := Split{}
  s.splitID = nrand()
  s.hostname = make([]string, 0)
  return &s
}