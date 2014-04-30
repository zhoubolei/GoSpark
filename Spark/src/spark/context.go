package spark

import (
  "time"
)

type Context struct {
    jobName    string
    scheduler  Scheduler
    initialzed bool
    started    bool
    startTime  time.Time
}


