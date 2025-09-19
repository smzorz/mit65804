package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
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
	for {
		args := RegisterArgs{}
		reply := RegisterReply{}
		ok := call("Coordinator.Register", &args, &reply)
		if ok {
			fmt.Printf("register success %v\n", reply.Filename)
		} else {
			fmt.Printf("register failed!\n")
		}
		if reply.Filename == "" {
			return
		}
		if reply.Type == MapTask {
			var encoders = make([]*json.Encoder, reply.NReduce)
			var tempfilenames = make([]string, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				//var filename = "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i) + ".json"
				tempFile, err := os.CreateTemp("", "mr-tmp-*")
				tempfilenames[i] = tempFile.Name()
				if err != nil {
					log.Fatalf("cannot create temp file")
				}
				defer tempFile.Close()

				encoders[i] = json.NewEncoder(tempFile)
			}

			//执行map任务
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))
			for _, kv := range kva {
				var reduceNumber = ihash(kv.Key) % reply.NReduce
				err := encoders[reduceNumber].Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write %v", reply.Filename)
				}
			}
			for i := 0; i < reply.NReduce; i++ {
				oname := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i) + ".json"
				err := os.Rename(tempfilenames[i], oname)
				if err != nil {
					log.Fatalf("cannot rename %v to %v", tempfilenames[i], oname)
				}
			}
			argsReport := ReportArgs{
				TaskID: reply.TaskID,
				Type:   MapTask,
			}
			replyReport := ReportReply{}
			ok = call("Coordinator.Report", &argsReport, &replyReport)
			if ok {
				fmt.Printf("report success %v\n", reply.Filename)
			} else {
				fmt.Printf("report failed!\n")
			}
		}
		time.Sleep(time.Second) //避免过于频繁的请求
		if reply.Type == ReduceTask {
			oname := "mr-out-" + strconv.Itoa(reply.TaskID)
			var decoders = make([]*json.Decoder, reply.NReduce)
			files := findIntermediateFilesForReduce(reply.TaskID, reply.Mapcount)
			for i := 0; i < reply.Mapcount; i++ {
				file, err := os.Open(files[i])
				if err != nil {
					log.Fatalf("cannot open %v", files[i])
				}
				defer file.Close()
				decoders[i] = json.NewDecoder(file)
			}
			ofile, _ := os.Create(oname)
			kva := []KeyValue{}
			for i := 0; i < reply.Mapcount; i++ {
				for {
					var kv KeyValue
					if err := decoders[i].Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			//排序
			sort.Sort(ByKey(kva))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()
			argsReport := ReportArgs{
				TaskID: reply.TaskID,
				Type:   ReduceTask,
			}
			replyReport := ReportReply{}
			ok = call("Coordinator.Report", &argsReport, &replyReport)
			if ok {
				fmt.Printf("report success %v\n", oname)
			} else {
				fmt.Printf("report failed!\n")
			}
		}
		// Your worker implementation here.

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
}

func findIntermediateFilesForReduce(reduceID int, mapCount int) []string {
	var files []string
	for i := 0; i < mapCount; i++ {
		files = append(files, fmt.Sprintf("mr-%d-%d.json", i, reduceID))
	}
	return files
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
