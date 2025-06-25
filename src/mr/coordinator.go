package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
)

type JobType int

const (
	Mapping JobType = iota
	Reducing
	Done
)

type JobStatus int

const (
	NotStarted JobStatus = iota
	InProgress
	DoneJob
)

type Job struct {
	Status  JobStatus
	JobType JobType

	MapID    int
	ReduceID int

	Filename string
}

type Coordinator struct {
	// Your definitions here.
	nReduce int

	status  JobType
	jobPool []Job

	mut sync.Mutex
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
	c.mut.Lock()
	defer c.mut.Unlock()

	reply.NReduce = c.nReduce

	// NOTE: Coordinator state transitions in CompletedJob() fn
	if c.status == Mapping || c.status == Reducing {
		info := c.grabFreeJob()
		if info == nil {
			// TODO: return WAIT STATE
			fmt.Printf("WAITING: On free job (%v)\n", c.status)
			return nil
		}
		reply.JobInfo = *info
	} else if c.status == Done {
		// removeIntermediateFiles()
		reply.JobInfo.JobType = Done
	} else {
		return errors.New("Reached unknown coordinator status")
	}

	return nil
}

func (c *Coordinator) CompletedJob(args *CompleteArgs, reply *GenericReply) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.jobPool[args.JobID].Status = DoneJob

	if c.status == Mapping && c.allJobsDone() {
		c.status = Reducing
		c.jobPool = make([]Job, 0)
		for i := 0; i < c.nReduce; i++ {
			c.jobPool = append(c.jobPool, Job{ReduceID: i, JobType: Reducing, Status: NotStarted})
		}
	} else if c.status == Reducing && c.allJobsDone() {
		c.status = Done
	}

	return nil
}

// WARN: Assumes that caller is inside lock
func (c *Coordinator) grabFreeJob() *Job {
	for _, j := range c.jobPool {
		if j.Status == NotStarted {
			j.Status = InProgress
			return &j
		}
	}
	return nil
}

// WARN: Assumes that caller is inside lock
func (c *Coordinator) allJobsDone() bool {
	for _, j := range c.jobPool {
		if j.Status != DoneJob {
			return false
		}
	}
	return true
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
	c.nReduce = nReduce

	for i, v := range files {
		c.jobPool = append(c.jobPool, Job{MapID: i, JobType: Mapping, Status: NotStarted, Filename: v})
	}

	c.server()
	return &c
}

func removeIntermediateFiles() {
	filenames, err := filepath.Glob("mr-out-*-*")
	if err != nil {
		fmt.Println(err)
		fmt.Printf("ERROR: Coordinator cannot find intermeidate files\n")
	}
	for _, f := range filenames {
		err := os.Remove(f)
		if err != nil {
			fmt.Println(err)
		}
	}
}
