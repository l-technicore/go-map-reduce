package main

/***************************************************************************************************
 * Author	: Lalit Kumar Singh(lalitkumarsingh@gmali.com)
 * Description	: This GO file contains code for a master node responsible for providing a
 *					 distributed map reduce functionality.
 * Debugging hours wasted : nil
 * P.S : Please update the debugging duration while Debugging
****************************************************************************************************/

import (
	"fmt"
	"encoding/json"
	"runtime/debug"
	"errors"
	"strings"
	"time"
	"flag"
	"sync"
    "bufio"
    "log"
    "net"
    "sync/atomic"
    "os"
)

var slaveDirectory sync.Map
var slaveCount int64
var jobs chan *Job

type Slave struct {
	socket net.Conn
}

type Job struct {
	data string
	result chan string
	err chan error
}


// Where it all begins.
func main() {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			stack = fmt.Sprintln(r) + "\n" + stack
			log.Println("Shutting Down Golang Map-Reduce Master due to unknown error (main)! : ", stack)
		}
	}()
	port := flag.String("port", "8100", "Listening Port for Master Node")
	flag.Parse()
	jobs = make(chan *Job, 100)
	
	// The Sort job that will run after 3 seconds after startup
	go DelayedJob()

	service := "0.0.0.0:"+*port
	listener, err := net.Listen("tcp", service)
	FailOnError(err)
	log.Println("Golang Map-Reduce Master's TCP server running!")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("..Error accepting TCP Connection ("+conn.RemoteAddr().String()+")! : " + err.Error())
			return
		}
		go (&Slave{
			socket: conn,
		}).Handler()
	}
}

func DelayedJob(){
	time.Sleep(3*time.Second)
	file, err := os.Open("list_of_strings.txt")
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
    SortByMapReduce(list)
}

func (s *Slave) Handler() {
	var job *Job
	defer func(){
		s.socket.Close()
		if r := recover(); r != nil {
			log.Println("Some unknown error occured: "+fmt.Sprint(r) + "===> StackTrace:" + string(debug.Stack()))
			job.err <- errors.New(fmt.Sprint(r))
		}
	}()
	s.Register()
	defer s.DeRegister()
	for job = range jobs {
		job.GetJobDone(s.socket)
	}
}

func (j *Job) GetJobDone(socket net.Conn) {
	if _, err := socket.Write([]byte(j.data+"\n")); err != nil {
		panic("Error sending the job to the slave node ("+socket.RemoteAddr().String()+")! : " + err.Error())
	}
	if result, err := bufio.NewReader(socket).ReadString('\n'); err != nil {
		panic("..Error reading result from slave node ("+socket.RemoteAddr().String()+")! : " + err.Error())
	} else {
		j.result <- result
	}
}

func (s *Slave) Register() {
	// slaveDirectory.Store(&s, s.job)
	atomic.AddInt64(&slaveCount, 1)
}

func (s *Slave) DeRegister() {
	// slaveDirectory.Delete(&s)
	atomic.AddInt64(&slaveCount, -1)
}

func SortByMapReduce(list []string) {
	slave_count := atomic.LoadInt64(&slaveCount)
	length := int64(len(list))
	start:=int64(0)
	end:=length/slave_count
	raw_results := make(chan string, slave_count)
	results := make([][]string, slave_count)

    log.Println("Starting Job Distribution!")
	for i:=int64(0); i<slave_count; i++ {
		// log.Println(end)
		mini_job, _ := json.Marshal(list[start:end])
		start = end
		if i == slave_count-int64(2) {
			end = length
		} else {
			end = end + length/slave_count
		}
		go JobChunk(string(mini_job), raw_results)
	}
    log.Println("Starting Result Collection!")
	for i:=int64(0); i<slave_count; i++ {
		var temp []string
		json.Unmarshal([]byte(<-raw_results), &temp)
		results[i] = temp
	}
	
	iter_results := make([]int, slave_count)

	min := ""
	max := ""
	who := int64(0)
	for i:=int64(0); i<slave_count; i++ {
		if max <= results[i][len(results[i])-1] {
			max = results[i][len(results[i])-1]
		}
	}
    log.Println("Here is the Sorted List of given",length,"elements:")
	for i:=int64(0); i<length; i++ {
		min = max
		for j:=int64(0); j<slave_count; j++ {
			if iter_results[j] < len(results[j]) && min >= results[j][iter_results[j]]{
				min = results[j][iter_results[j]]
				who = j
			}
		}
		iter_results[who]++
		fmt.Println(min)
	}
}

func max(x,y int64) int64 {
	if x > y {
		return x
	}
	return y
}
func JobChunk(mini_job string, raw_results chan string) {
	job := &Job{
		data: mini_job,
		result: make(chan string, 1),
		err: make(chan error, 1),
	}
	for {
		jobs <- job
		select{
		case res := <-job.result:
			raw_results<-res
			return
		case <-job.err:
		}
	}
}

func FailOnError(err error) {
	if err != nil {
		log.Fatal("Fatal Error:", err.Error())
	}
}