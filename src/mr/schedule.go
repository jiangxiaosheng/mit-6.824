package mr

import (
	"context"
	"path/filepath"
	"stathat.com/c/consistent"
	"sync"
	"sync/atomic"
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
	s.distributeMapTasks(mapTasks)
	reduceTasks := s.genReduceTasks(j, mapTasks)
	s.distributeReduceTasks(reduceTasks)
}

// distributeTasks assumes that master and workers are using a shared file system like GFS
// or local file system. So it only needs to tell receivers the location of the files rather than
// the binary data.
func (s *Scheduler) distributeMapTasks(tasks []*mapTask) {
	if len(tasks) == 0 {
		return
	}

	completedTasks := int32(0)
	wg := sync.WaitGroup{}
	jobname := tasks[0].jobName

	for _, t := range tasks {
		wg.Add(1)
		go func(t *mapTask) {
			defer wg.Done()

			Debugf("map task %v is assigned to worker %v", t.taskName(), t.workerAddr)
			req := &DoMapTaskRequest{
				InputFile:     t.inputFile,
				ExecutableApp: t.executableApp,
				JobName:       t.jobName,
				TaskId:        t.taskId,
				TaskName:      t.taskName(),
				NReduce:       t.nReduce,
			}
			reply := &DoMapTaskResponse{}
			if s.master.callWorker(t.workerAddr, "Worker.DoMapTask", req, reply) == false {
				Errorf("distributing map task %v to worker %v failed", t.taskName(), t.workerAddr)
			}
			if reply.State == Successful {
				atomic.AddInt32(&completedTasks, 1)
				Infof("job %v is in map stage, %v/%v map task completed", jobname, atomic.LoadInt32(&completedTasks), len(tasks))
			}
			// TODO: fail-over
		}(t)
	}
	wg.Wait()
	Infof("job %v finished map stage, starting reduce stage", jobname)
}

func (s *Scheduler) distributeReduceTasks(tasks []*reduceTask) {
	if len(tasks) == 0 {
		return
	}

	jobname := tasks[0].jobName
	wg := &sync.WaitGroup{}

	completedTasks := int32(0)
	for _, t := range tasks {
		wg.Add(1)
		go func(t *reduceTask) {
			defer wg.Done()

			Debugf("reduce task %v is assigned to worker %v", t.taskName(), t.workerAddr)
			req := &DoReduceTaskRequest{
				InputDir:       t.inputDir,
				ExecutableApp:  t.executableApp,
				JobName:        t.jobName,
				TaskId:         t.taskId,
				TaskName:       t.taskName(),
				MapOutputInfos: t.mapOutputInfos,
				OutputDir:      t.outputDir,
			}
			reply := &DoReduceTaskResponse{}
			if s.master.callWorker(t.workerAddr, "Worker.DoReduceTask", req, reply) == false {
				Errorf("distributing reduce task %v to worker %v failed", t.taskName(), t.workerAddr)
			}
			if reply.State == Successful {
				atomic.AddInt32(&completedTasks, 1)
				Infof("job %v is in reduce stage, %v/%v reduce task completed", jobname, atomic.LoadInt32(&completedTasks), len(tasks))
			}

		}(t)
	}

	wg.Wait()
	Infof("job %v finished reduce stage", jobname)
}

func (s *Scheduler) genMapTasks(j job) []*mapTask {
	tasks := make([]*mapTask, 0)
	id := 0

	s.cHash.RLock()
	for _, i := range j.input {
		t := &mapTask{
			inputFile:     i,
			taskId:        id,
			jobName:       j.name,
			executableApp: j.executableApp,
			nReduce:       j.nReduce,
		}
		t.workerAddr, _ = s.cHash.Get(t.taskName())
		id++
		tasks = append(tasks, t)
	}
	s.cHash.RUnlock()
	Debugf("generated %v map tasks", len(tasks))
	return tasks
}

func (s *Scheduler) genReduceTasks(j job, maptasks []*mapTask) []*reduceTask {
	tasks := make([]*reduceTask, 0)
	id := 0

	mapOutputInfo := make([]MapOutputInfo, 0)
	for _, t := range maptasks {
		mapOutputInfo = append(mapOutputInfo, MapOutputInfo{
			MapTaskId:  t.taskId,
			WorkerAddr: t.workerAddr,
		})
	}

	s.cHash.RLock()
	for ; id < j.nReduce; id++ {
		t := &reduceTask{
			jobName:        j.name,
			taskId:         id,
			inputDir:       filepath.Join(tempDir, j.name),
			outputDir:      j.outputDir,
			executableApp:  j.executableApp,
			mapOutputInfos: mapOutputInfo,
		}
		t.workerAddr, _ = s.cHash.Get(t.taskName())
		tasks = append(tasks, t)
	}
	s.cHash.RUnlock()
	return tasks
}

func getAvailableWorkersAddr(wi workerInfos) []string {
	ret := make([]string, 0)
	for _, w := range wi {
		ret = append(ret, w.address)
	}
	return ret
}
