package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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
			// fmt.Printf("DONE: Mapping \"%v\" file\n", reply.JobInfo.Filename)
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
		// NOTE: Intermediate file name: 'mr-inter-X-Y' X is map ID and Y is reduce ID
		outFilename := fmt.Sprintf("mr-inter-%v-%v", reply.JobInfo.MapID, i)

		file, err := os.Create(outFilename) // Create/truncate the file
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", outFilename, err)
		}
		defer file.Close() // Ensure the file is closed

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ") // Optional: for pretty-printing the output JSON

		// Encode the KeyValue slice directly to the file stream.
		err = encoder.Encode(intermeidate[i])
		if err != nil {
			return fmt.Errorf("failed to encode JSON to file %s: %w", outFilename, err)
		}
	}

	return nil
}

func reduceFiles(reducef func(string, []string) string, reply JobReply) error {
	// NOTE: Intermediate file name: 'mr-inter-X-Y' X is map ID and Y is reduce ID
	path := fmt.Sprintf("mr-inter-*-%v", reply.JobInfo.ReduceID)
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
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Printf("Error creating file '%v'\n", err)
	}

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

	ofile.Close()

	return nil
}

func readKV(filePath string) ([]KeyValue, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", filePath, err)
		return []KeyValue{}, err
	}
	decoder := json.NewDecoder(file)

	var data []KeyValue
	// Decode the JSON data directly from the file stream into the KeyValue slice.
	err = decoder.Decode(&data)
	if err != nil {
		// io.EOF means the file was empty or malformed at the beginning,
		// but if we expect an array, an empty file would likely error out differently.
		// For reading a single top-level JSON value, io.EOF is common for empty inputs.
		// Here, if the file is truly empty or not valid JSON, we'll get a parsing error.
		if err == io.EOF {
			return []KeyValue{}, nil // Return an empty slice if the file was empty JSON (e.g., [])
		}
		return nil, fmt.Errorf("failed to decode JSON from file %s: %w", filePath, err)
	}

	file.Close()
	return data, nil
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
