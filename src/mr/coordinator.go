package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Status int

const (
	Mapping Status = iota
	Reducing
	Done
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int
	allFilenames   []string
	mapProgress    int
	reduceProgress int

	status Status
	mut    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GiveJob(args *GenericArgs, reply *JobReply) error {
	// TODO: keep track if all maps are done! (this way a reduce worker doesn't start before all map workers are done)
	// TODO: Change how to sorting works so that ihash() determines which file to put the reduced key into

	c.mut.Lock()
	defer c.mut.Unlock()

	reply.JobType = c.status
	reply.NReduce = c.nReduce

	if c.status == Mapping {
		reply.Filename = c.allFilenames[c.mapProgress]
		reply.MapID = c.mapProgress

		fmt.Println("hi")
		c.mapProgress++
		if c.mapProgress >= len(c.allFilenames) {
			c.status = Reducing
		}
	} else if c.status == Reducing {
		reply.RedTaskNum = c.reduceProgress

		c.reduceProgress++
		if c.reduceProgress >= c.nReduce {
			c.status = Done
		}
	} else if c.status == Done {
	} else {
		return errors.New("Reached unknown coordinator status")
	}

	return nil
}

func (c *Coordinator) CompletedMap(args *CompleteArgs, reply *GenericReply) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.status == Done {
		fmt.Println("Coordinator is DONE")
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.allFilenames = files
	c.nReduce = nReduce
	c.mapProgress = 0

	c.server()
	return &c
}
