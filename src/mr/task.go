package mr

import "fmt"

type stage int

const (
	submitted stage = iota
	mapping
	reducing
	completed
)

type task interface {
	taskName() string
}

type mapTask struct {
	inputFile     string
	taskId        int
	workerAddr    string
	jobName       string
	executableApp string
	nReduce       int
}

func (m *mapTask) taskName() string {
	return fmt.Sprintf("%v-maptask-%v", m.jobName, m.taskId)
}

type reduceTask struct {
	jobName        string
	taskId         int
	inputDir       string
	outputDir      string
	workerAddr     string
	executableApp  string
	mapOutputInfos []MapOutputInfo
}

func (r *reduceTask) taskName() string {
	return fmt.Sprintf("%v-reducetask-%v", r.jobName, r.taskId)
}
