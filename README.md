# go-map-reduce
Distributing task to golang nodes for processing

## How to run?
1. Run any number of slaves by executiong:
   - go run slave.go -master "0.0.0.0:8100"
2. Run only one master. (also reads and dispatches jobs for list_of_strings.txt file containing unsorted names)
   - go run master.go -port "8100"

## Whtat's going on in there?
slave.go runs the slave node, which keeps on trying to connect the master node specified using the -master flag. There can be any number of slave nodes connected to the master node. But there is only one master.

In case a slave node dies the task it was performing is given to some other still connected slave node. Slave nodes can be added or removed at any time. But there should be atleast one slave node active for the sort job to be done else the job execution will pause until one slave is available (excluding the master).

master.go reads list of 1000 names from the "list_of_strings.txt" file in the same directory after 3 secons from the start of execution. It then distributes the equally divided chunks of the original list to all the connected slave nodes in JSON format over TCP.

This JSON is read by the slave node through the TCP connection they have to the master node, sorting fot the same is done after unmarshal and then the result is sent back to the master through the TCP connection as well.
