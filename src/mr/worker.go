package mr

import (
	"context"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	"strconv"
)

const (
	MasterAddr = "127.0.0.1:4567"
)

var (
	tempDir, _ = filepath.Abs("../../.mrtemp")
)

type Worker struct {
	Hostname string
	Host     string
	Port     int
	context  context.Context
	client   *rpc.Client
}

func (w *Worker) getAddr() string {
	return fmt.Sprintf("%v:%v", w.Host, w.Port)
}

func (w *Worker) Serve() error {
	rpc.Register(w)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", ":"+strconv.Itoa(w.Port))

	if err != nil {
		log.Fatal("listen error: ", err)
		return err
	}
	Infof("worker is running on %v:%v", w.Host, w.Port)

	s := &http.Server{Handler: nil}
	go func() {
		select {
		case <-w.context.Done():
			_ = s.Close()
			_ = w.client.Close()
		}
	}()

	go func() {
		w.keepHeartbeat()
	}()

	req := &RegisterRequest{
		Address:  fmt.Sprintf("%v:%v", w.Host, w.Port),
		Hostname: w.Hostname,
	}

	w.callMaster("Master.RegisterWorker", req, &struct{}{})

	return s.Serve(l)
}

func (w *Worker) DoMapTask(req *DoMapTaskRequest, resp *DoMapTaskResponse) error {
	mapf, _ := loadPlugin(req.ExecutableApp)
	Infof("worker %v:%v received map task %v", w.Host, w.Port, req.TaskName)

	inputContents, err := ioutil.ReadFile(req.InputFile)
	if err != nil {
		Errorf("reading contents of input file %v failed: %v", req.InputFile, err)
		return err
	}

	kvs := mapf(req.InputFile, string(inputContents))

	spill(kvs, req.NReduce, req.JobName, req.TaskId)

	resp.State = Successful
	return nil
}

func (w *Worker) DoReduceTask(req *DoReduceTaskRequest, resp *DoReduceTaskResponse) error {
	_, reducef := loadPlugin(req.ExecutableApp)
	Infof("worker %v:%v received reduce task %v", w.Host, w.Port, req.TaskName)

	outputs := make([]string, 0)

	for _, info := range req.MapOutputInfos {
		fetchReq := &FetchMapOutputRequest{
			MapTaskId:    info.MapTaskId,
			ReduceTaskId: req.TaskId,
			JobName:      req.JobName,
		}
		fetchReply := &FetchMapOutputResponse{}

		if w.callWorker(info.WorkerAddr, "Worker.FetchMapOutput", fetchReq, fetchReply) == false {
			Errorf("fetch map output from worker %v failed", info.WorkerAddr)
		}
		outputs = append(outputs, fetchReply.MapOutputPath)
		Debugf("%v", outputs)
	}

	sortedKVs := merge(outputs)

	fn := fmt.Sprintf("%v-%v", req.JobName, req.TaskId)
	outputFile, err := os.Create(filepath.Join(req.OutputDir, fn))
	if err != nil {
		Errorf("open output file %v failed, %v", fn, err)
		return err
	}
	i := 0
	for i < len(sortedKVs) {
		j := i + 1
		for j < len(sortedKVs) && sortedKVs[j].Key == sortedKVs[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, sortedKVs[k].Value)
		}
		output := reducef(sortedKVs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(outputFile, "%v %v\n", sortedKVs[i].Key, output)
		if err != nil {
			Errorf("write KeyValues to file %v failed, %v", fn, err)
			return err
		}

		i = j
	}

	return nil
}

// FetchMapOutput assumes all workers mounting a shared file system, so just sets the output file path.
func (w *Worker) FetchMapOutput(req *FetchMapOutputRequest, resp *FetchMapOutputResponse) error {
	path := filepath.Join(tempDir, req.JobName, fmt.Sprintf("%v-%v-%v", req.JobName, req.MapTaskId, req.ReduceTaskId))
	resp.MapOutputPath = path
	return nil
}

func NewWorker(ctx context.Context, host string) (*Worker, error) {
	w := new(Worker)
	hn, err := os.Hostname()
	if err != nil {
		log.Fatal("get hostname error: ", err)
		return nil, err
	}
	w.Hostname = hn
	w.Host = host
	w.Port = getFreePort()
	w.context = ctx
	w.client, err = rpc.DialHTTP("tcp", MasterAddr)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	return w, nil
}

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ihash
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// call
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func (w *Worker) callMaster(rpcname string, args interface{}, reply interface{}) bool {
	err := w.client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	Errorln(err)
	return false
}

func (w *Worker) callWorker(workerAddr string, rpcname string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("tcp", workerAddr)
	if err != nil {
		Errorln("dialing:", err)
		return false
	}

	err = client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	Errorln(err)
	return false
}

func getFreePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v, error: %v", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
