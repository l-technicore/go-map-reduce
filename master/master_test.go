package main

import (
	"bufio"
	"log"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func SetupServerAndClient(service string) (conn net.Conn, client net.Conn, err error) {
	var listener net.Listener

	// Setup TCP Listener

	listener, err = net.Listen("tcp", service)
	if err != nil {
		log.Fatal(err)
	}

	// Accept TCP connections from slaves

	var getConn = make(chan net.Conn, 1)
	go func() {
		conn, err = listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		getConn <- conn
	}()

	// Register a slave

	if client, err = net.Dial("tcp", service); err != nil {
		log.Fatal(err)
	}

	go func() {
		bufio.NewReader(client).ReadString('\n')
	}()

	conn = <-getConn
	return
}

func TestSlaveHandler(t *testing.T) {
	var conn, client, _ = SetupServerAndClient("0.0.0.0:8100")
	defer conn.Close()
	defer client.Close()
	var s = &Slave{socket: conn}

	// Handling a hoax tcp socket

	go s.Handler()

	// Test handler registration

	time.Sleep(time.Second)
	if atomic.LoadInt64(&slaveCount) != 1 {
		t.Error("Slave not registered after calling handler for the slave")
	}

	close(jobs)
	time.Sleep(100 * time.Millisecond)

	// Test handler de-registration

	if atomic.LoadInt64(&slaveCount) != 0 {
		t.Error("Slave did not deregister after closing of handler for the slave")
	}
}

func TestGetJobDone(t *testing.T) {
	var err error
	var conn, client, _ = SetupServerAndClient("0.0.0.0:8101")
	defer conn.Close()
	defer client.Close()

	// Closing the socket
	conn.Close()

	var job *Job = &Job{
		data:   `{"sort","me"}`,
		result: make(chan string, 1),
		err:    make(chan error, 1),
	}

	go job.GetJobDone(conn)

	// Check if error was successfully reported

	err = <-job.err

	if err == nil {
		t.Error("Slave did not throw an error when connection was unavailable")
	}
}

func TestRegister(t *testing.T) {
	var conn, client, _ = SetupServerAndClient("0.0.0.0:8102")
	defer conn.Close()
	defer client.Close()
	var s = &Slave{socket: conn}

	// Test Registration

	s.Register()

	if atomic.LoadInt64(&slaveCount) != 1 {
		t.Error("Error in slave registeration!")
	}

	// Reset global slave count

	atomic.StoreInt64(&slaveCount, 0)
}

func TestDeRegister(t *testing.T) {
	var conn, client, _ = SetupServerAndClient("0.0.0.0:8103")
	defer conn.Close()
	defer client.Close()
	var s = &Slave{socket: conn}

	// Set slave counter to 1

	atomic.StoreInt64(&slaveCount, 1)

	s.DeRegister()

	if atomic.LoadInt64(&slaveCount) != 0 {
		t.Error("Error in slave deregisteration!")
	}

	// Reset global slave count

	atomic.StoreInt64(&slaveCount, 0)
}
