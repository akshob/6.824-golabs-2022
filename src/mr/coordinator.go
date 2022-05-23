package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files   []string
	reducePartitions int
	workers map[int]WorkerState
}

func (c *Coordinator) AskForTask(args *WorkerArgs, reply *WorkerReply) error {
	if _, ok := c.workers[args.Id]; !ok {
		c.workers[args.Id] = WorkerState(Idle)
	}
	reply.KeepAlive = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	len, count := 0, 0
	for _, v := range c.workers {
		len++
		if v == WorkerState(Done) {
			count++
		}
	}

	if count > 0 && count == len {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files = files
	c.reducePartitions = nReduce

	c.server()
	return &c
}
