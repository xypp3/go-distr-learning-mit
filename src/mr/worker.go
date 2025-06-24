package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for true {
		reply, err := callJob()
		if err {
			fmt.Println("Terminating cause of 'call()' error")
			return
		}

		switch reply.JobInfo.JobType {
		case Mapping:
			// fmt.Printf("Mapping \"%v\" file\n", reply.JobInfo.Filename)

			err := mapFile(mapf, reply)
			if err != nil {
				fmt.Println(err)
				fmt.Printf("ERROR: Mapping JobID:%v --- filename: \"%v\" file\n", reply.JobInfo.MapID, reply.JobInfo.Filename)
				return
			}

			cargs := CompleteArgs{JobID: reply.JobInfo.MapID}
			r := GenericReply{}
			if !call("Coordinator.CompletedJob", &cargs, &r) {
				fmt.Printf("ERROR: coordintor not received completion: %v\n", r.Msg)
				return
			}
			// fmt.Printf("DONE: Mapping \"%v\" file\n", reply.Filename)
		case Reducing:
			// fmt.Printf("Reducing %v/%v in progress\n", reply.JobInfo.ReduceID, reply.NReduce-1)

			err := reduceFiles(reducef, reply)
			if err != nil {
				fmt.Println(err)
				fmt.Printf("ERROR: Reducing JobID:%v\n", reply.JobInfo.ReduceID)
				return
			}

			cargs := CompleteArgs{JobID: reply.JobInfo.ReduceID}
			r := GenericReply{}
			if !call("Coordinator.CompletedJob", &cargs, &r) {
				fmt.Printf("ERROR: coordintor not received completion: %v\n", r.Msg)
				return
			}
			// fmt.Printf("DONE: Reducing %v/%v in progress\n", reply.JobInfo.ReduceID, reply.NReduce-1)
		case Done:
			// fmt.Println("Done working")
			return
		}
	}

}

func mapFile(mapf func(string, string) []KeyValue, reply JobReply) error {
	bytesFile, err := os.ReadFile(reply.JobInfo.Filename)
	if err != nil {
		return err
	}

	mapped := mapf(reply.JobInfo.Filename, string(bytesFile))

	intermeidate := make([][]KeyValue, reply.NReduce)
	for _, record := range mapped {
		intermeidate[ihash(record.Key)%reply.NReduce] = append(intermeidate[ihash(record.Key)%reply.NReduce], record)
	}

	for i := 0; i < reply.NReduce; i++ {
		tmp := ""
		for _, r := range intermeidate[i] {
			tmp += fmt.Sprintf("%v %v\n", r.Key, r.Value)
		}

		// NOTE: Intermediate file name: 'mr-out-X-Y' X is map ID and Y is reduce ID
		outFilename := fmt.Sprintf("mr-out-%v-%v", reply.JobInfo.MapID, i)
		out_f, _ := os.Create(outFilename)
		fmt.Fprint(out_f, tmp)
		out_f.Close()
	}

	return nil
}

func reduceFiles(reducef func(string, []string) string, reply JobReply) error {
	// NOTE: Intermediate file name: 'mr-out-X-Y' X is map ID and Y is reduce ID
	path := fmt.Sprintf("mr-out-*-%v", reply.JobInfo.ReduceID)
	filenames, err := filepath.Glob(path)
	if err != nil {
		fmt.Println(err)
		fmt.Printf("ERROR: Reducing %v/%v in progress\n", reply.JobInfo.ReduceID, reply.NReduce-1)
		return errors.New("ERROR: Reduce cannot find file\n")
	}

	tmp := []KeyValue{}
	for _, v := range filenames {
		kv, err := readKV(v)
		if err != nil {
			fmt.Println(err)
			fmt.Printf("ERROR: Reducing %v/%v during file reading\n", reply.JobInfo.ReduceID, reply.NReduce-1)
			return errors.New("ERROR: Reduce cannt read file")
		}

		tmp = append(tmp, kv...)
	}
	sort.Sort(ByKey(tmp))

	oname := fmt.Sprintf("mr-out-%v", reply.JobInfo.ReduceID)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	//
	// call Reduce on each distinct key in tmp[],
	// and print the result to mr-out-*.
	//
	i := 0
	for i < len(tmp) {
		j := i + 1
		for j < len(tmp) && tmp[j].Key == tmp[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, tmp[k].Value)
		}
		output := reducef(tmp[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", tmp[i].Key, output)

		i = j
	}

	return nil
}

func readKV(filePath string) ([]KeyValue, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", filePath, err)
		return []KeyValue{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	kvs := []KeyValue{}

	lineNumber := 0
	for scanner.Scan() { // Reads the next line
		lineNumber++
		line := scanner.Text() // Get the text of the current line

		// Trim leading/trailing whitespace from the line
		trimmedLine := strings.TrimSpace(line)

		// Skip empty lines after trimming
		if trimmedLine == "" {
			fmt.Printf("Line %d (empty, skipped).\n", lineNumber)
			continue
		}

		// Split the line by one or more spaces
		// strings.Fields splits by one or more whitespace characters (space, tab, newline, etc.)
		// and also handles multiple spaces between words correctly, returning non-empty strings.
		parts := strings.Fields(trimmedLine)
		tmp := KeyValue{}
		tmp.Key = parts[0]
		tmp.Value = parts[1]

		kvs = append(kvs, tmp)
	}

	return kvs, nil
}

func callJob() (JobReply, bool) {
	args := GenericArgs{}
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
