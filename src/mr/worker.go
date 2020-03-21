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
	"time"
)

const WorkerWait = 100 * time.Millisecond

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	log.Printf("worker started, registering with master")
	workerId := CallRegisterWorker()
	log.Printf("worker registered as #%v", workerId)
	for {
		log.Printf("asking master for task")
		task := CallGetTask(workerId)
		log.Printf("task %v#%v retrieved, doing task", phaseName(task.Phase), task.TaskNum)
		doTask(task, mapf, reducef)
		time.Sleep(WorkerWait)
	}
}

//
// do a task, dispatch the correct function depending on task phase
//
func doTask(task *Task, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	switch task.Phase {
	case Map:
		doMapTask(task, mapf)
	case Reduce:
		doReduceTask(task, reducef)
	case Terminated:
		os.Exit(0)
	default:
		log.Fatalf("unexpected task phase: %v", phaseName(task.Phase))
	}
}

func doMapTask(task *Task, mapf func(string, string) []KeyValue) {
	if task.Phase != Map {
		log.Fatalf("unexpected task phase: %v", phaseName(task.Phase))
	}

	// read file content
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()

	// map file content to []KeyValue
	kva := mapf(task.Filename, string(content))

	// make nReduce buckets of KeyValues
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	for idx, bucket := range buckets {
		tempFile, err := ioutil.TempFile("./", "tmp_map")
		if err != nil {
			log.Printf("error creating temp file: %v", err)
			CallSubmitTask(task, false)
			tempFile.Close()
			return
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range bucket {
			if err := enc.Encode(&kv); err != nil {
				log.Printf("failed to encode kv: %v", err)
				CallSubmitTask(task, false)
				tempFile.Close()
				return
			}
		}
		tempFile.Close()
		err = os.Rename(tempFile.Name(), mapOutFilename(task.TaskNum, idx))
		if err != nil {
			log.Printf("failed to rename tmp file: %v", err)
			CallSubmitTask(task, false)
			return
		}
	}
	CallSubmitTask(task, true)
}

func doReduceTask(task *Task, reducef func(string, []string) string) {
	var kva []KeyValue
	for idx := 0; idx < task.NMap; idx++ {
		filename := mapOutFilename(idx, task.TaskNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	if kva == nil {
		outFile, _ := os.Create(reduceOutFilename(task.TaskNum))
		outFile.Close()
		CallSubmitTask(task, true)
		return
	}

	sort.Sort(ByKey(kva))
	outFile, _ := os.Create(reduceOutFilename(task.TaskNum))
	i := 0

	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	outFile.Close()
	CallSubmitTask(task, true)
}

func CallRegisterWorker() int {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	if !call("Master.RegisterWorker", &args, &reply) {
		log.Println("can't reach master, terminating")
		os.Exit(0)
	}
	return reply.WorkerId
}

func CallGetTask(workerId int) *Task {
	args := GetTaskArgs{}
	args.WorkerId = workerId
	reply := GetTaskReply{}
	if !call("Master.GetTask", &args, &reply) {
		log.Println("can't reach master, terminating")
		os.Exit(0)
	}
	task := reply.Task
	log.Printf("task %v#%v received", phaseName(task.Phase), task.TaskNum)
	return &task
}

func CallSubmitTask(task *Task, success bool) {
	args := SubmitTaskArgs{}
	args.Success = success
	args.Task = *task
	reply := SubmitTaskReply{}
	if !call("Master.SubmitTask", &args, &reply) {
		log.Println("can't reach master, terminating")
		os.Exit(0)
	}
	log.Printf("task %v#%v submitted", phaseName(task.Phase), task.TaskNum)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
