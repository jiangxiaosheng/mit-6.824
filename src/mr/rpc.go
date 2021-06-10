package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

type RegisterRequest struct {
	Address  string
	Hostname string
}

type RegisterResponse struct {
}

type HeartbeatRequest struct {
	WorkerAddr      string
	LastHealthyTime time.Time
}

type HeartbeatResponse struct {
}

type RunMRRequest struct {
	JobName   string
	Inputs    []string
	OutputDir string
	NReduce   int

	// filename of the go plugin which provides map and reduce function, usually suffixed by .so
	ExecutableApp string
}

type RunMRResponse struct {
}

type TaskState int

const (
	Successful TaskState = iota
	Failed
)

type DoMapTaskRequest struct {
	InputFile     string
	ExecutableApp string
	JobName       string
	TaskId        int
	TaskName      string
	NReduce       int
}

type DoMapTaskResponse struct {
	State TaskState
}

type MapOutputInfo struct {
	MapTaskId  int
	WorkerAddr string
}

type DoReduceTaskRequest struct {
	InputDir       string
	ExecutableApp  string
	JobName        string
	TaskId         int
	TaskName       string
	OutputDir      string
	MapOutputInfos []MapOutputInfo
}

type DoReduceTaskResponse struct {
	State TaskState
}

type FetchMapOutputRequest struct {
	MapTaskId    int
	ReduceTaskId int
	JobName      string
}

type FetchMapOutputResponse struct {
	MapOutputPath string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
