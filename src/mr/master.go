package mr

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	jobs            []string
	heartbeatServer *HeartbeatServer
	context         context.Context
	scheduler       *Scheduler
	rpcClients      map[string]*rpc.Client
}

type workerInfo struct {
	hostname string
	address  string
}

type workerInfos []workerInfo

type job struct {
	name          string
	startDate     time.Time
	endDate       time.Time
	state         stage
	input         []string
	output        []string
	executableApp string
}

func (m *Master) RunMR(req *RunMRRequest, resp *RunMRResponse) error {
	Debugln("job name is ", req.JobName)
	j := job{
		name:          req.JobName,
		startDate:     time.Now(),
		state:         submitted,
		input:         req.Inputs,
		executableApp: req.ExecutableApp,
	}
	m.scheduler.Schedule(j)
	return nil
}

// availableWorkers
func (m *Master) availableWorkers() workerInfos {
	return m.heartbeatServer.workerInfos
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RegisterWorker(req *RegisterRequest, _ *RegisterResponse) error {
	m.heartbeatServer.workerInfos = append(m.heartbeatServer.workerInfos, workerInfo{
		hostname: req.Hostname,
		address:  req.Address,
	})

	Infof("total %v workers registered\n", len(m.heartbeatServer.workerInfos))
	for _, i := range m.heartbeatServer.workerInfos {
		Infof("worker hostname: %v, address: %v\n", i.hostname, i.address)
	}

	ct := time.Now()
	m.heartbeatServer.healthCheckpoints[req.Address] = &ct
	go func() {
		m.healthCheck(m.context, req.Address)
	}()

	return nil
}

func (m *Master) healthCheck(ctx context.Context, worker string) {
	ticker := time.NewTicker(healthCheckInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.heartbeatServer.mu.RLock()
			Debugf("checking healthy state: total %v workers available\n", len(m.heartbeatServer.workerInfos))
			lastTime := *m.heartbeatServer.healthCheckpoints[worker]
			m.heartbeatServer.mu.RUnlock()
			if time.Since(lastTime) > timeout {
				Debugf("worker %v is unhealthy, so it is deleted from the available workers", worker)
				m.heartbeatServer.mu.Lock()
				delete(m.heartbeatServer.healthCheckpoints, worker)
				m.removeWorker(worker)
				m.heartbeatServer.mu.Unlock()

				m.heartbeatServer.mu.RLock()
				Debugf("currently %v workers available: %v", len(m.heartbeatServer.workerInfos), m.heartbeatServer.workerInfos)
				m.heartbeatServer.mu.RUnlock()

				return
			}
		}
	}
}

func (m *Master) callWorker(workerAddr, rpcName string, args, reply interface{}) bool {
	if m.rpcClients[workerAddr] == nil {
		c, err := rpc.DialHTTP("tcp", workerAddr)
		if err != nil {
			Errorln("dialing:", err)
			return false
		}
		m.rpcClients[workerAddr] = c
	}

	err := m.rpcClients[workerAddr].Call(rpcName, args, reply)
	if err == nil {
		return true
	}
	Errorln(err)
	return false
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server(ctx context.Context) error {
	rpc.Register(m)
	rpc.Register(m.heartbeatServer)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":4567")

	if e != nil {
		log.Fatal("listen error:", e)
		return e
	}
	s := &http.Server{Handler: nil}

	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		}
	}()

	return s.Serve(l)
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (m *Master) init() {
	m.heartbeatServer = &HeartbeatServer{
		mu:                sync.RWMutex{},
		workerInfos:       make(workerInfos, 0),
		healthCheckpoints: make(map[string]*time.Time),
	}
	m.scheduler = newScheduler(m)
	m.scheduler.init(m.context)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(ctx context.Context, files []string, nReduce int) {
	m := Master{}
	m.context = ctx
	m.rpcClients = make(map[string]*rpc.Client)

	// Your code here.
	m.init()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := m.server(ctx)
		if err != nil {
			log.Fatal("master server error: ", err)
		}
	}()

	wg.Wait()
}
