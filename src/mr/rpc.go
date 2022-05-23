package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerState int
const (
	Idle WorkerState = iota
	InProgress
	Done
)

type WorkerArgs struct {
	Id int
	State WorkerState
}

type WorkerTask int
const (
	Map WorkerTask = iota
	Reduce
)

type WorkerReply struct {
	Task WorkerTask
	File string
	ReducePartitions int
	KeepAlive bool
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
