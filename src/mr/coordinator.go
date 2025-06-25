package mr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
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
	Waiting
	DoneJob
)

type Job struct {
	Status    JobStatus
	JobType   JobType
	StartedAt time.Time

	MapID    int
	ReduceID int

	Filename string
}

type Coordinator struct {
	// Your definitions here.
	nReduce         int
	timeoutDuration time.Duration

	status  JobType
	jobPool []Job

	mut         sync.Mutex
	cancelTimer context.CancelFunc
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
			reply.JobInfo.Status = Waiting
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

	// NOTE: This is if worker doesn't crash but finishes job too slow
	// e.g. worker A takes 20sec, but coordinator waits 10s and gives it to worker B
	// worker B completes it in 5s and marks job done but worker A still sends job later
	if c.jobPool[args.JobID].Status == DoneJob {
		return nil
	}
	// TODO: Do above with context passing to worker

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
	for i, j := range c.jobPool {
		if j.Status == NotStarted {
			c.jobPool[i].Status = InProgress
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
		c.cancelTimer()
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
	c.timeoutDuration = 10 * time.Second

	for i, v := range files {
		c.jobPool = append(c.jobPool, Job{MapID: i, JobType: Mapping, Status: NotStarted, Filename: v})
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancelTimer = cancel
	go c.startTimeoutMonitor(ctx)

	c.server()
	return &c
}

func (c *Coordinator) startTimeoutMonitor(ctx context.Context) {
	ticker := time.NewTicker(c.timeoutDuration / 2) // Check more frequently than the timeout itself
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Time to check for expired jobs
			c.mut.Lock() // Lock to safely iterate and modify jobs
			fmt.Println("checking crashed jobs")
			for i, job := range c.jobPool {
				if job.Status == InProgress { // Only check active jobs
					if time.Since(job.StartedAt) > c.timeoutDuration {
						// This job has been active for too long!
						c.jobPool[i].Status = NotStarted // Make it available again
					}
				}
			}
			c.mut.Unlock()
		case <-ctx.Done():
			fmt.Println("Coordinator: Timeout monitor stopped.")
			return // Exit the goroutine when context is canceled
		}
	}
}

func removeIntermediateFiles() {
	filenames, err := filepath.Glob("mr-inter-*-*")
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
