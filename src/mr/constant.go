package mr

import "time"

const (
	ChunkSize            = 1 << 16         // 64 Kb
	readStreamBufferSize = 1 << 10         // 1 Kb
	timeout              = 6 * time.Second // for unhealthy worker detection
	heartbeatInterval    = 2 * time.Second
	healthCheckInterval  = heartbeatInterval
)
