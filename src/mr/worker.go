package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// TODO:: PSEUDOCODE
	// 1. Call coordinator to get params
	// 2. Execute mapf(p1, p2,)
	// 3. Split into nReduce intermediate files
	// 4. End (? send completed status)

	// uncomment to send the Example RPC to the coordinator.
	reply, err := CallJob()
	if err {
		fmt.Println("Terminating cause of 'call()' error")
		return
	}

	switch reply.JobType {
	case Mapping:
		fmt.Printf("Mapping \"%v\" file\n", reply.Filename)

		err := mapFile(mapf, reply)
		if err != nil {
			fmt.Println(err)
			fmt.Printf("ERROR: Mapping \"%v\" file\n", reply.Filename)
			return
		}

		fmt.Printf("DONE: Mapping \"%v\" file\n", reply.Filename)
	case Reducing:
		fmt.Printf("Reducing %v/%v in progress\n", reply.ProgReduce, reply.NReduce)
	case Done:
		fmt.Println("Done working")
	}

}

func mapFile(mapf func(string, string) []KeyValue, reply JobReply) error {
	bytesFile, err := os.ReadFile(reply.Filename)
	if err != nil {
		return err
	}

	mapped := mapf(reply.Filename, string(bytesFile))
	size := (len(mapped) / reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		outFilename := fmt.Sprintf("%v-m-out-%v", reply.Filename, i)
		out_f, _ := os.Create(outFilename)
		tmp := ""

		if i+1 == reply.NReduce {
			j := 0
			for (size*i)+j < len(mapped) {
				tmp += fmt.Sprintf("%v %v\n", mapped[(size*i)+j].Key, mapped[(size*i)+j].Value)
				j++
			}
		} else {
			for j := 0; j < size; j++ {
				tmp += fmt.Sprintf("%v %v\n", mapped[(size*i)+j].Key, mapped[(size*i)+j].Value)
			}
		}

		fmt.Fprint(out_f, tmp)
		out_f.Close()
	}

	return nil
}

func CallJob() (JobReply, bool) {
	args := JobArgs{}
	reply := JobReply{}

	ok := call("Coordinator.GiveJob", &args, &reply)
	if ok {
		return reply, false
	} else {
		return reply, true
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
