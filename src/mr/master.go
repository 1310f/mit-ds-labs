package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const WorkerTimeout = 10 * time.Second
const MasterLoopInterval = 100 * time.Millisecond

type TaskInfo struct {
	State     TaskState
	Worker    int
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	mu         sync.Mutex
	nReduce    int
	filenames  []string
	phase      Phase
	taskInfos  []TaskInfo
	taskCh     chan Task
	numWorkers int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	//log.Println("blocked, RegisterWorker")
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.WorkerId = m.numWorkers
	m.numWorkers += 1
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	workerId := args.WorkerId
	log.Printf("worker#%v calls GetTask", workerId)
	var task Task
	task = <-m.taskCh
	// must not lock while waiting for channel
	m.mu.Lock()
	if m.phase != Terminated {
		m.taskInfos[task.TaskNum].StartTime = time.Now()
		m.taskInfos[task.TaskNum].Worker = workerId
		m.taskInfos[task.TaskNum].State = InProgress
	} else {
		task = Task{Phase: Terminated}
	}
	m.mu.Unlock()
	reply.Task = task
	log.Printf("assigned task %v#%v to worker#%v", phaseName(m.phase), task.TaskNum, workerId)
	return nil
}

func (m *Master) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := args.Task
	if args.Success {
		if m.taskInfos[task.TaskNum].State == Completed {
			log.Printf("task %v#%v is already completed, ignoring", phaseName(task.Phase), task.TaskNum)
		} else {
			m.taskInfos[task.TaskNum].State = Completed
			log.Printf("task %v#%v complete", phaseName(task.Phase), task.TaskNum)
		}
	} else {
		m.taskInfos[task.TaskNum].State = Idle
		log.Printf("task %v#%v failed, rescheduling", phaseName(task.Phase), task.TaskNum)
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase == Terminated
}

//
// main loop
//
func (m *Master) loop() {
	m.startMapPhase()
	for !m.updateTaskStates() {
		time.Sleep(MasterLoopInterval)
	}
	log.Println("map phase complete")
	m.startReducePhase()
	for !m.updateTaskStates() {
		time.Sleep(MasterLoopInterval)
	}
	log.Println("reduce phase complete")
	m.mu.Lock()
	m.phase = Terminated
	m.mu.Unlock()
	log.Println("all tasks finished, shutting down...")
}

func (m *Master) updateTaskStates() bool {
	//log.Println("blocked, updateTaskStates")
	m.mu.Lock()
	defer m.mu.Unlock()
	// go through all tasks, update their states
	// returns true if all tasks are completed
	allTasksCompleted := true
	for idx, info := range m.taskInfos {
		if info.State != Completed {
			allTasksCompleted = false
		}
		switch info.State {
		case Idle:
			log.Printf("task %v#%v idle, queueing", phaseName(m.phase), idx)
			m.taskCh <- m.makeTask(idx)
			m.taskInfos[idx].State = Queued
		case Queued:
			//log.Printf("task %v#%v waiting in queue", phaseName(m.phase), idx)
		case InProgress:
			if time.Now().Sub(info.StartTime) > WorkerTimeout {
				log.Printf("task %v#%v timed out, reset as idle", phaseName(m.phase), idx)
				m.taskInfos[idx].State = Idle
			}
		case Completed:
		default:
			log.Fatalf("unexpected task state: %v", info.State)
		}
	}
	return allTasksCompleted
}

func (m *Master) makeTask(taskNum int) Task {
	task := Task{
		TaskNum: taskNum,
		Phase:   m.phase,
		NMap:    len(m.filenames),
		NReduce: m.nReduce,
	}
	switch m.phase {
	case Map:
		task.Filename = m.filenames[taskNum]
	case Reduce:
	default:
		log.Fatalf("illegal master phase: %v", phaseName(m.phase))
	}
	return task
}

func (m *Master) startMapPhase() {
	//log.Println("blocked, startMapPhase")
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Println("master starting map phase")
	m.phase = Map
	m.taskInfos = make([]TaskInfo, len(m.filenames))
}

func (m *Master) startReducePhase() {
	//log.Println("blocked, startReducePhase")
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Println("master starting reduce phase")
	m.phase = Reduce
	m.taskInfos = make([]TaskInfo, m.nReduce)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.filenames = files
	// channel needs to be created before go routine executes
	var chanBufferSize int
	if len(m.filenames) > m.nReduce {
		chanBufferSize = len(m.filenames)
	} else {
		chanBufferSize = m.nReduce
	}
	m.taskCh = make(chan Task, chanBufferSize)
	go m.loop()

	m.server()
	return &m
}
