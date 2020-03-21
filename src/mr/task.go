package mr

import "fmt"

const (
	Map = iota
	Reduce
	Terminated
)

type Phase int

const (
	Idle = iota
	Queued
	InProgress
	Completed
)

type TaskState int

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
