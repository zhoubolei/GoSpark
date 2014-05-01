package spark

type TextFileRDD struct {
    BaseRDD
    path      string
    size      int64
    splitSize int64
}


func (t *TextFileRDD) compute(split Split) Yielder {
    yielder := make(chan interface{}, 100)
    
    return yielder
}

func (t *TextFileRDD) init(ctx *Context, path string, numSplits int) {
    t.BaseRDD.init(ctx, t)
    t.path = path

}

func (t *_TextFileRDD) String() string {
    return fmt.Sprintf("TextFileRDD-%d <%s %d>", t.id, t.path, t.len())
}

func newTextFileRDD(ctx *Context, path string) RDD {
    textRdd := &_TextFileRDD{}
    textRdd.init(ctx, path, env.parallel)
    return textRdd
}
