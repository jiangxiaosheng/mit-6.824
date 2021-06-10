package main

import (
	"../mr"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

func toAbsPath(fn string) string {
	ret, _ := filepath.Abs(fn)
	return ret
}

func cleanOutputDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func randomizeJobName(jn string) string {
	return fmt.Sprintf("%v-%v", jn, time.Now().UnixNano())
}

var (
	jobName       = flag.String("jobname", "default-mr", "Optional. the name of the job")
	nReduce       = flag.Int("nReduce", 1, "Optional. the number of reducers")
	inputFiles    = flag.String("inputs", "", `Required. input files, supporting the "*" wildcard character`)
	outputDir     = flag.String("output", "", `Required. the directory where the outputs will be written to`)
	overwrite     = flag.Bool("overwrite", false, `Optional. if true, the directory specified by the "output" flag will be cleaned in the beginning`)
	executableApp = flag.String("app", "", "Required. the file build in go-plugin mode which contains both map and reduce function (like wc.so)")
)

func checkFlagsSet() bool {
	if *inputFiles == "" || *outputDir == "" || *executableApp == "" {
		return false
	}
	return true
}

func main() {
	flag.Parse()
	if checkFlagsSet() == false {
		flag.Usage()
		os.Exit(1)
	}

	if *overwrite {
		err := cleanOutputDir(toAbsPath(*outputDir))
		if err != nil {
			panic(err)
		}
	}
	inputs, err := filepath.Glob(*inputFiles)
	if err != nil {
		panic(err)
	}

	c, err := rpc.DialHTTP("tcp", mr.MasterAddr)
	if err != nil {
		mr.Debugf("dialing:", err)
	}
	req := &mr.RunMRRequest{
		JobName:       randomizeJobName(*jobName),
		Inputs:        inputs,
		OutputDir:     toAbsPath(*outputDir),
		ExecutableApp: toAbsPath(*executableApp),
		NReduce:       *nReduce,
	}
	resp := &mr.RunMRResponse{}
	c.Call("Master.RunMR", req, resp)
}
