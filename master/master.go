package main

/***************************************************************************************************
 * Author	: Lalit Kumar Singh(lalitkumarsingh@gmali.com)
 * Description	: This GO file contains code for a master node responsible for providing a
 *					 distributed map reduce functionality.
 * Debugging hours wasted : nil
 * P.S : Please update the debugging duration while Debugging
****************************************************************************************************/

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var slaveDirectory sync.Map
var slaveCount int64
var jobs chan *Job = make(chan *Job, 100)
var fileName *string

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
			log.Printf("shutting down due to unknown error (main)! : %s\n%s\n", r, debug.Stack())
		}
	}()
	var port = flag.String("port", "8100", "Listening Port for Master Node")
	fileName = flag.String("file", "list_of_strings.txt", "Listening Port for Master Node")
	flag.Parse()

	// The Sort job fired 3 seconds after startup

	go DelayedJob()

	// Setting up TCP Listner
	StartTcpServer(*port)
}

func StartTcpServer(port string) {
	var service = "0.0.0.0:" + port
	listener, err := net.Listen("tcp", service)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("TCP server is up & running!")

	// Accept TCP connections from slaves and assign them a handler

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		// Open a handler for slave node

		go (&Slave{
			socket: conn,
		}).Handler()
	}
}

func DelayedJob() {
	time.Sleep(3 * time.Second)

	file, err := os.Open(*fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var list []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		list = append(list, strings.ToLower(scanner.Text()))
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Get the job done by the distributed slave nodes

	SortByMapReduce(list)
}

func (s *Slave) Handler() {
	var job *Job

	defer func() {
		s.socket.Close()
		if r := recover(); r != nil {
			log.Printf("closing socket, error: %s\n, stack trace: %s\n", r, debug.Stack())
			job.err <- errors.New(fmt.Sprint(r))
		}
	}()

	s.Register()
	defer s.DeRegister()

	// Send job to the slave node being handled

	for job = range jobs {
		job.GetJobDone(s.socket)
	}
}

func (s *Slave) Register() {
	atomic.AddInt64(&slaveCount, 1)
}

func (s *Slave) DeRegister() {
	atomic.AddInt64(&slaveCount, -1)
}

func (j *Job) GetJobDone(socket net.Conn) {
	// Send the job to respective slave node

	if _, err := socket.Write([]byte(j.data + "\n")); err != nil {
		j.err <- err
	}

	// Ready to recieve result when done

	if result, err := bufio.NewReader(socket).ReadString('\n'); err != nil {
		j.err <- err
	} else {
		j.result <- result
	}
}

func SortByMapReduce(list []string) {
	var start, i, j, who int64
	var err error
	var min, max string
	var batch []byte
	var slaveCount = atomic.LoadInt64(&slaveCount)
	var rawResults = make(chan string, slaveCount)
	var results = make([][]string, slaveCount)
	var length = int64(len(list))
	var end = length / slaveCount

	log.Println("Starting Job Distribution!")

	for ; i < slaveCount; i++ {
		if batch, err = json.Marshal(list[start:end]); err != nil {
			log.Fatal(err)
		}
		start = end
		if i == slaveCount-2 {
			end = length
		} else {
			end = end + length/slaveCount
		}
		go JobChunk(string(batch), rawResults)
	}

	log.Println("Starting Result Collection!")

	for i = 0; i < slaveCount; i++ {
		var temp []string
		if err := json.Unmarshal([]byte(<-rawResults), &temp); err != nil {
			log.Fatal(err)
		}
		results[i] = temp
	}

	iterateResults := make([]int, slaveCount)

	for i = 0; i < slaveCount; i++ {
		if max <= results[i][len(results[i])-1] {
			max = results[i][len(results[i])-1]
		}
	}

	log.Printf("Here is the sorted list of names for given %d input:", length)

	for i = 0; i < length; i++ {
		min = max
		for j = 0; j < slaveCount; j++ {
			if iterateResults[j] < len(results[j]) && min >= results[j][iterateResults[j]] {
				min = results[j][iterateResults[j]]
				who = j
			}
		}
		iterateResults[who]++
		fmt.Println(min)
	}
}

func JobChunk(batch string, rawResults chan string) {
	job := &Job{
		data:   batch,
		result: make(chan string, 1),
		err:    make(chan error, 1),
	}

	// Send jobs to the slave nodes through respective handlers and retry in case of error

	for {
		jobs <- job
		select {
		case res := <-job.result:
			rawResults <- res
			return
		case <-job.err:
		}
	}
}
