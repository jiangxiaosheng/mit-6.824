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
	JobName string
	Inputs  []string

	// filename of the go plugin which provides map and reduce function, usually named with .so
	ExecutableApp string
}

type RunMRResponse struct {
}

type DoMapTaskRequest struct {
	InputFile     string
	ExecutableApp string
	JobName       string
	TaskId        int
	TaskName      string
}

type DoMapTaskResponse struct {
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
