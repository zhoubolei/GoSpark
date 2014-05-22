MIT EECS 6.824: Distributed Systems 
==========
Spring 2014, course page: http://css.csail.mit.edu/6.824/2014/

Course project: GoSpark: An In-Memory Distributed Computation Platform in Go

Kuan-Ting Yu, Jiasi Shen, Bolei Zhou


Install Go
---------
```sh
mkdir ~/software  # -> this is where I install go
cd ~/software
hg clone -u release https://code.google.com/p/go
cd go/src
./all.bash
```

Environment Setup
--------

Add these in your .bashrc

```sh
export HADOOP_HOME=/usr/local/hadoop   #-> where you install the hadoop
export DSPROJECT_HOME=$HOME/824project   #-> where you put our project
export JAVA_HOME=/usr/lib/jvm/java-6-openjdk/   #-> where is the jdk installed

lzohead () {
	hadoop dfs -cat $1 | lzop -dc | head -1000 | less
}

unalias dfs &> /dev/null
alias dfs="hdfs dfs"
unalias hls &> /dev/null
alias hls="dfs -ls"
export PATH=$PATH:$HADOOP_HOME/bin
# Add Hadoop bin/ directory to PATH
export CLASSPATH=${DSPROJECT_HOME}/HDFSSplitUtil/bin:`hadoop classpath`:${CLASSPATH}

# For Go
export GOROOT=$HOME/software/go
export GOPATH=${DSPROJECT_HOME}/Spark
export PATH=$PATH:$HOME/software/go/bin
```

Testing
--------

Update master host name (and/or port number) in config.txt.

On scheduler machine, run:
```sh
go test -run KMeans -timeout 1h
```
```sh
go test -run MasterMRLineCount -debug
```

On worker machines, run:
```sh
go test -run BasicWorker -timeout 1h
```
```sh
go test -run WorkerSuperUnrel -timeout 1h -debug
```

Options:
* `-run` (required) chooses which application to run. 
  - Scheduler: `-run KMeans`, `-run MasterMRLineCount`, etc.
  - Workers: `-run BasicWorker` for reliable, `-run WorkerRPCUnrel` for unreliable network connection, or `-run WorkerSuperUnrel` for unreliable network & may crash.
* `-timeout` sets the timeout. For example, `-timeout 1h` for 1 hour.
* `-debug` prints verbose log.



Utility Shell
--------
install tmux: sudo apt-get install tmux

get into tmux, then run the sh
```sh
tmux
cd shells
./start-all.sh
```
