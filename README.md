824project
==========


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

On master machine, run:

```sh
go test -run MasterMRLineCount -timeout 1h  # 1h for 1hour
```
or
```sh
go test -run KMeans -timeout 1h 
```

On worker machines, run:

```sh
go test -run BasicWorker -timeout 1h  
```
or
```sh
go test -run WorkerRPCUnrel -timeout 1h  
```
where workers randomly discard RPC requests and replies, or
```sh
go test -run WorkerSuperUnrel -timeout 1h  
```
where workers not only have reliable RPC but also crash and reboot.


Utility Shell
--------
install tmux: sudo apt-get install tmux

get into tmux, then run the sh
```sh
tmux
cd shells
./start-all.sh
```
