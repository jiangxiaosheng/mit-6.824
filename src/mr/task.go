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
	outputFile    string
	taskId        int
	workerAddr    string
	jobName       string
	executableApp string
}

func (m *mapTask) taskName() string {
	return fmt.Sprintf("%v-maptask-%v", m.jobName, m.taskId)
}

type reduceTask struct {
}

func doMap() {

}

func doReduce() {

}
