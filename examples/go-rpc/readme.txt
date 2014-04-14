Demonstrates how to implement RPC over TCP in Go.

To compile and run:

  1. In one terminal, run:                go run server.go
     - Server now listens to port 1234
  2. In another terminal, run:            go run client.go
     - Client dials port 1234
     - To use separate machines, change the IP address in client.go
  3. After client connects to server, it prints result = 15 and exits
     - The arithmetic was computed on the server machine
  4. Shut down the server manually

More information:

  * Documentation: [[http://golang.org/pkg/net/rpc/]]
  * Example source code: [[https://github.com/daviddengcn/go-rpc]]

