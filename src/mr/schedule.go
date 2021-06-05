package mr

import (
	"context"
	"fmt"
	"stathat.com/c/consistent"
	"time"
)

type Scheduler struct {
	cHash  *consistent.Consistent
	master *Master
}

func newScheduler(m *Master) *Scheduler {
	s := &Scheduler{
		cHash:  consistent.New(),
		master: m,
	}
	return s
}

func (s *Scheduler) init(ctx context.Context) {
	s.cHash = consistent.New()

	updateWorkersDaemon := func() {
		ticker := time.NewTicker(timeout)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.cHash.Set(getAvailableWorkersAddr(s.master.availableWorkers()))
			}
		}
	}
	go updateWorkersDaemon()
}

func (s *Scheduler) Schedule(j job) {
	Infof("scheduling job %v", j.name)
	mapTasks := s.genMapTasks(j)
	s.distributeTasks(mapTasks)
}

// distributeTasks assumes that master and workers are using a shared file system like GFS
// or local file system. So it only needs to tell receivers the location of the files rather than
// the binary data.
func (s *Scheduler) distributeTasks(tasks []*mapTask) {
	if len(tasks) == 0 {
		return
	}
	for _, t := range tasks {
		worker, _ := s.cHash.Get(t.taskName())
		Infof("task %v is assigned to worker %v", t.taskName(), worker)
		req := &DoMapTaskRequest{
			InputFile:     t.inputFile,
			ExecutableApp: t.executableApp,
			JobName:       t.jobName,
			TaskId:        t.taskId,
			TaskName:      t.taskName(),
		}
		reply := &DoMapTaskResponse{}
		if s.master.callWorker(worker, "Worker.DoMapTask", req, reply) == false {
			Errorf("distributing task %v to worker %v failed", t.taskName(), worker)
		}
	}
}

func (s *Scheduler) genMapTasks(j job) []*mapTask {
	tasks := make([]*mapTask, 0)
	id := 0

	s.cHash.RLock()
	for _, i := range j.input {
		w, _ := s.cHash.Get(i)
		t := &mapTask{
			inputFile:     i,
			outputFile:    fmt.Sprintf("map-%v-%v", j.name, id),
			taskId:        id,
			workerAddr:    w,
			jobName:       j.name,
			executableApp: j.executableApp,
		}
		id++
		tasks = append(tasks, t)
	}
	s.cHash.RUnlock()
	Debugf("generated %v map tasks", len(tasks))
	return tasks
}

func mapStage(j job) {

}

func reduceStage() {

}

func getAvailableWorkersAddr(wi workerInfos) []string {
	ret := make([]string, 0)
	for _, w := range wi {
		ret = append(ret, w.address)
	}
	return ret
}
