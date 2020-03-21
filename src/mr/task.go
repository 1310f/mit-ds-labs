package mr

import "fmt"

type Phase int

const (
	Map Phase = iota
	Reduce
	Terminated
)

type TaskState int

const (
	Idle TaskState = iota
	Queued
	InProgress
	Completed
)

type Task struct {
	TaskNum   int
	Phase     Phase
	Filename  string
	NMap      int
	NReduce   int
	Terminate bool
}

func phaseName(phase Phase) string {
	switch phase {
	case Map:
		return "map"
	case Reduce:
		return "reduce"
	case Terminated:
		return "terminated"
	default:
		return "unknown"
	}
}

func mapOutFilename(mapTaskNum int, reduceTaskNum int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskNum, reduceTaskNum)
}

func reduceOutFilename(reduceTaskNum int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskNum)
}
