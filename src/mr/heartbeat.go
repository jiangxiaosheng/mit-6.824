package mr

import (
	"sync"
	"time"
)

type healthCheckpoints map[string]*time.Time

type HeartbeatServer struct {
	mu sync.RWMutex
	workerInfos
	healthCheckpoints healthCheckpoints
}

func (hbs *HeartbeatServer) Heartbeat(req HeartbeatRequest, resp *HeartbeatResponse) error {
	hbs.mu.Lock()
	hbs.healthCheckpoints[req.WorkerAddr] = &req.LastHealthyTime
	Debugf("heartbeat from %v, last healthy time %v", req.WorkerAddr, req.LastHealthyTime)
	hbs.mu.Unlock()

	return nil
}

func (w *Worker) keepHeartbeat() {
	sendHeartbeat := func() {
		req := HeartbeatRequest{
			WorkerAddr:      w.getAddr(),
			LastHealthyTime: time.Now(),
		}
		w.callMaster("HeartbeatServer.Heartbeat", req, &struct{}{})
		Debugf("send heartbeat, current time is %v", req.LastHealthyTime)
	}

	ticker := time.NewTicker(heartbeatInterval)
	for {
		select {
		case <-ticker.C:
			sendHeartbeat()
		case <-w.context.Done():
			return
		}
	}
}

func (m *Master) removeWorker(worker string) {
	idx := 0
	for i, info := range m.heartbeatServer.workerInfos {
		if info.address == worker {
			idx = i
			break
		}
	}
	m.heartbeatServer.workerInfos = append(m.heartbeatServer.workerInfos[:idx], m.heartbeatServer.workerInfos[idx+1:]...)
}
