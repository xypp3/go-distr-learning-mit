package mr

import (
	"bufio"
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
		reply, err := CallJob()
		if err {
			fmt.Println("Terminating cause of 'call()' error")
			return
		}

		switch reply.JobType {
		case Mapping:
			// fmt.Printf("Mapping \"%v\" file\n", reply.Filename)

			err := mapFile(mapf, reply)
			if err != nil {
				fmt.Println(err)
				fmt.Printf("ERROR: Mapping \"%v\" file\n", reply.Filename)
				return
			}

			// fmt.Printf("DONE: Mapping \"%v\" file\n", reply.Filename)
		case Reducing:
			// fmt.Printf("Reducing %v/%v in progress\n", reply.ProgReduce, reply.NReduce-1)

			path := fmt.Sprintf("*-m-out-%v", reply.RedTaskNum)
			filenames, err := filepath.Glob(path)
			if err != nil {
				fmt.Println(err)
				fmt.Printf("ERROR: Reducing %v/%v in progress\n", reply.RedTaskNum, reply.NReduce-1)
				return
			}

			tmp := []KeyValue{}
			for _, v := range filenames {
				kv, err := readKV(v)
				if err != nil {
					fmt.Println(err)
					fmt.Printf("ERROR: Reducing %v/%v during file reading\n", reply.RedTaskNum, reply.NReduce-1)
					return
				}

				tmp = append(tmp, kv...)
			}
			sort.Sort(ByKey(tmp))

			oname := fmt.Sprintf("mr-out-%v", reply.RedTaskNum)
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

			// fmt.Printf("DONE: Reducing %v/%v in progress\n", reply.ProgReduce, reply.NReduce-1)
		case Done:
			// fmt.Println("Done working")
			return
		}
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
		outFilename := fmt.Sprintf("mr-out-%v-%v", reply.MapTaskNum, i)
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
