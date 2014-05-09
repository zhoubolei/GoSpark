// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package websocket

import (
        "testing"
        "io"
        "net/http"
        "fmt"
        "strconv"
        "math/rand"
        "strings"
)

// Echo the data received on the WebSocket.
func EchoServer(ws *Conn) {
        fmt.Printf("received %v\n", *ws)
        io.Copy(ws, ws)
}

func webHandler(ws *Conn) {
    var in []byte
    if err := Message.Receive(ws, &in); err != nil {
        fmt.Printf("error %v\n", err)
        return
    }
    fmt.Printf("Received: %s\n", string(in))
    //Message.Send(ws, in)
/*
    out := []byte("that's it")
    err := Message.Send(ws, out)
    if err != nil {fmt.Print("send err %s\n", err)}
    err = Message.Send(ws, in)
    if err != nil {fmt.Print("send err %s\n", err)}
*/
    n1 := strconv.Itoa(rand.Intn(100))
    n2 := strconv.Itoa(rand.Intn(100))
    out := []byte(strings.Join([]string{n1, n2}, " "))
    Message.Send(ws, out)
}

func strHandler(ws *Conn) {
  var message string
  Message.Receive(ws, &message)
  fmt.Printf("received: %s\n", message)
  message = "hello"
  err := Message.Send(ws, message)
  if err != nil {fmt.Print("send err %s\n", err)}
}

// This example demonstrates a trivial echo server.
func TestHandler(t *testing.T) {
        fmt.Printf("running handler\n")
        //http.Handle("/echo", Handler(EchoServer))
        http.Handle("/echo", Handler(webHandler))
        //http.Handle("/echo", Handler(strHandler))
        //err := http.ListenAndServe(":12345", Handler(EchoServer))
        err := http.ListenAndServe(":12345", Handler(webHandler))
        //err := http.ListenAndServe(":12345", Handler(strHandler))
        if err != nil {
                panic("ListenAndServe: " + err.Error())
        }
}
