package main

import (
	"../mr"
	"net/rpc"
	"path/filepath"
)

func toAbsPath(fn string) string {
	ret, _ := filepath.Abs(fn)
	return ret
}

func main() {
	c, err := rpc.DialHTTP("tcp", mr.MasterAddr)
	if err != nil {
		mr.Debugf("dialing:", err)
	}
	req := &mr.RunMRRequest{
		JobName:       "test-wc",
		Inputs:        []string{toAbsPath("pg-grimm.txt")},
		ExecutableApp: toAbsPath("wc.so"),
	}
	resp := &mr.RunMRResponse{}
	c.Call("Master.RunMR", req, resp)
}
