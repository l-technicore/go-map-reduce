package main

/***************************************************************************************************
 * Author	: Lalit Kumar Singh(lalitkumarsingh@gmali.com)
 * Description	: This GO file contains code for a slave node responsible for executing
 *					 the given job and returning the same post sorting
 * Debugging hours wasted : nil
 * P.S : Please update the debugging duration while Debugging
****************************************************************************************************/

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

var slaveDirectory sync.Map
var slaveCount uint64
var jobs chan *Job

type Slave struct {
	socket net.Conn
}

type Job struct {
	data   string
	result chan string
	err    chan error
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			stack = fmt.Sprintln(r) + "\n" + stack
			log.Println("shutting down due to unknown error (main)! : ", stack)
		}
	}()
	var service = flag.String("master", "0.0.0.0:8100", "Master Node's Remote Address")
	flag.Parse()

	DialTcpServer(*service)
}

func DialTcpServer(service string) {
	var retry = false
	var err error
	for {
		var conn net.Conn
		if conn, err = net.Dial("tcp", service); err != nil {
			if !retry {
				fmt.Printf("\n%s, Retrying.", err.Error())
				retry = true
			} else {
				fmt.Printf(".")
			}
		} else {
			retry = false
			log.Println("\nsubscribed to the master node successfully!")
			startAcceptingJobs(conn)
		}
		time.Sleep(time.Second)
	}
}

func startAcceptingJobs(socket net.Conn) {
	defer func() {
		socket.Close()
		if r := recover(); r != nil {
			log.Printf("some unknown error occured: %s\nstack trace:%s\n", r, debug.Stack())
		}
	}()

	var job string
	var err error
	var batch []string
	var result []byte

	// Execute jobs received from master until some error is encountered
	for {
		// Ready to receive a job from master node
		if job, err = bufio.NewReader(socket).ReadString('\n'); err != nil {
			log.Println(err)
			return
		}

		log.Println("Recieved a job!")
		if err = json.Unmarshal([]byte(job), &batch); err != nil {
			log.Println(err)
			return
		}

		// Sorting the recieved batch
		log.Println("Started Sorting!")
		sort.StringSlice(batch).Sort()

		if result, err = json.Marshal(batch); err != nil {
			log.Println(err)
			return
		}

		// Sending sorted array back to the master
		if _, err = socket.Write([]byte(string(result) + "\n")); err != nil {
			log.Println(err)
			return
		}
		log.Println("Result Shared with Master!\n")
	}
}
