package main

/***************************************************************************************************
 * Author	: Lalit Kumar Singh(lalitkumarsingh@gmali.com)
 * Description	: This GO file contains code for a slave node responsible for executing
 *					 the given job and returning the same post sorting
 * Debugging hours wasted : nil
 * P.S : Please update the debugging duration while Debugging
****************************************************************************************************/

import (
	"fmt"
	"flag"
	"encoding/json"
	"runtime/debug"
	"sync"
	"sort"
	"time"
    "bufio"
    "log"
    "net"
)

var slaveDirectory sync.Map
var slaveCount uint64
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
	service := flag.String("master", "0.0.0.0:8100", "Master Node's Remote Address")
	flag.Parse()
	retry := false
	for{
		if conn, err := net.Dial("tcp", *service); err != nil {
			if !retry {
				fmt.Print("\n"+err.Error()+"\t Retrying.")
				retry = true
			} else {
				fmt.Print(".")
			}
		} else {
			retry = false
			fmt.Print("\n")
			log.Println("Slave subscribed the Golang Map-Reduce master successfully!")
			StartAcceptingJobs(conn)
		}
		time.Sleep(time.Second)
	}
}

func StartAcceptingJobs(socket net.Conn) {
	defer func(){
		socket.Close()
		if r := recover(); r != nil {
			log.Println("Some unknown error occured: "+fmt.Sprint(r) + "===> StackTrace:" + string(debug.Stack()))
		}
	}()
	for {
		if job, err := bufio.NewReader(socket).ReadString('\n'); err != nil {
			log.Println("..Error reading job from master node or connection lost! ("+socket.RemoteAddr().String()+")! : " + err.Error())
			break
		} else {
			var my_list []string
			log.Println("Recieved a job!")
			if err := json.Unmarshal([]byte(job), &my_list); err != nil {
				log.Fatal("JSON Unmarshal Error!")
			}
			log.Println("Started Sorting!")
			sort.StringSlice(my_list).Sort()
			// log.Println("Sorted List!", my_list)
			my_result, _ := json.Marshal(my_list)
			if _, err := socket.Write([]byte(string(my_result)+"\n")); err != nil {
				log.Println("Error sending the sorted result to the master node or connection lost! ("+socket.RemoteAddr().String()+")! : " + err.Error())
				break
			}
			log.Println("Result Shared with Master!\n")
		}
	}
}