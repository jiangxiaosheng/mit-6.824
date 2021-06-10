package mr

import (
	"container/heap"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type iterableKVs struct {
	kvs ByKey
	i   int
}

func newIterableKVs(kvs *[]KeyValue) *iterableKVs {
	return &iterableKVs{
		kvs: *(*ByKey)(kvs),
		i:   0,
	}
}

func (it *iterableKVs) next() error {
	if it.i == len(it.kvs)-1 {
		return fmt.Errorf("iterableKVs has reached the bottom")
	}
	it.i++
	return nil
}

func (it *iterableKVs) get() (KeyValue, error) {
	if it.i == len(it.kvs) {
		return KeyValue{}, fmt.Errorf("iterableKVs has reached the bottom")
	}
	return it.kvs[it.i], nil
}

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type iterableKVsHeap []*iterableKVs

func (it iterableKVsHeap) Len() int { return len(it) }
func (it iterableKVsHeap) Swap(i, j int) {
	it[i], it[j] = it[j], it[i]
}
func (it iterableKVsHeap) Less(i, j int) bool {
	ki, erri := it[i].get()
	kj, errj := it[j].get()
	if erri != nil && errj != nil {
		return true
	} else if erri != nil {
		return false
	} else if errj != nil {
		return true
	} else {
		return ki.Key < kj.Key
	}
}
func (it *iterableKVsHeap) Push(x interface{}) {
	*it = append(*it, x.(*iterableKVs))
}
func (it *iterableKVsHeap) Pop() interface{} {
	old := *it
	n := len(old)
	x := old[n-1]
	*it = old[0 : n-1]
	return x
}

func spill(kvs []KeyValue, nReduce int, jobName string, mapId int) {
	part := partition(kvs, nReduce)
	mapTempDir := filepath.Join(tempDir, jobName)
	initOutputDir(mapTempDir)

	wg := &sync.WaitGroup{}
	for i, p := range part {
		wg.Add(1)
		go func(i int, p []KeyValue) {
			defer wg.Done()
			outputFile, err := os.Create(filepath.Join(mapTempDir, fmt.Sprintf("%v-%v-%v", jobName, mapId, i)))
			if err != nil {
				Errorf("create file %v failed, %v", outputFile, err)
				return
			}

			sort.Sort(ByKey(p))
			enc := gob.NewEncoder(outputFile)
			err = enc.Encode(p)
			if err != nil {
				Errorf("gob encode data to file %v failed, %v", outputFile, err)
				return
			}
		}(i, p)
	}
	wg.Wait()
	Infof("map task %v completed", fmt.Sprintf("%v-maptask-%v", jobName, mapId))
}

func partition(kvs []KeyValue, nReduce int) [][]KeyValue {
	part := make([][]KeyValue, nReduce)
	for _, k := range kvs {
		i := ihash(k.Key) % nReduce
		part[i] = append(part[i], k)
	}
	return part
}

func readMapOutput(path string) ([]KeyValue, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	dec := gob.NewDecoder(file)
	var ret []KeyValue
	err = dec.Decode(&ret)
	return ret, nil
}

// merge is executed on reduce's side.
func merge(outputs []string) []KeyValue {
	allkvs := make([][]KeyValue, 0)
	for _, output := range outputs {
		kvs, err := readMapOutput(output)
		if err != nil {
			Errorf("read map output file %v failed: ", err)
		}
		allkvs = append(allkvs, kvs)
	}
	return multiSourceMergeSort(allkvs)
}

func multiSourceMergeSort(kvs [][]KeyValue) []KeyValue {
	itKVsHeap := make(iterableKVsHeap, 0)

	heap.Init(&itKVsHeap)
	for _, kv := range kvs {
		t := newIterableKVs(&kv)
		heap.Push(&itKVsHeap, t)
	}

	ret := make([]KeyValue, 0)
	completed := len(kvs)

	for {
		minKV := itKVsHeap[0]
		ele, _ := minKV.get()
		ret = append(ret, ele)
		if err := minKV.next(); err != nil {
			completed--
			if completed == 0 {
				return ret
			}
			heap.Pop(&itKVsHeap)
			continue
		}
		hasLarger := false
		for {
			nxt, _ := minKV.get()
			if nxt.Key != ele.Key {
				hasLarger = true
				break
			}
			ret = append(ret, nxt)
			if err := minKV.next(); err != nil {
				completed--
				if completed == 0 {
					return ret
				}
				break
			}
		}
		heap.Pop(&itKVsHeap)
		if hasLarger {
			heap.Push(&itKVsHeap, minKV)
		}
	}
}

func initOutputDir(outputDir string) {
	_, err := os.Stat(outputDir)
	if err == nil {
		return
	}
	if os.IsExist(err) {
		return
	}
	os.MkdirAll(outputDir, os.ModePerm)
}
