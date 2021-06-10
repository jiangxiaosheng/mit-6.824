package mr

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"stathat.com/c/consistent"
	"strings"
	"testing"
	"time"
	"unicode"
)

func init() {
	DebugMode = true
}

func TestConsistentHashing(t *testing.T) {
	c := consistent.New()
	c.Add("123")
	c.Add("456")
	fmt.Println(c.Members())
	fmt.Println(c.Get("123"))
	fmt.Println(c.Get("123"))
}

func TestHeartbeat(t *testing.T) {
	doWork := func(
		done <-chan interface{},
		pulseInterval time.Duration,
	) (<-chan interface{}, <-chan time.Time) {
		heartbeat := make(chan interface{})
		results := make(chan time.Time)
		go func() {
			defer close(heartbeat)
			defer close(results)

			pulse := time.Tick(pulseInterval)
			workGen := time.Tick(2 * pulseInterval)

			sendPulse := func() {
				select {
				case heartbeat <- struct{}{}:
				default:
				}
			}
			sendResult := func(r time.Time) {
				for {
					select {
					case <-done:
						return
					case <-pulse:
						sendPulse()
					case results <- r:
						return
					}
				}
			}

			for i := 0; i < 2; i++ {
				select {
				case <-done:
					return
				case <-pulse:
					sendPulse()
				case r := <-workGen:
					sendResult(r)
				}
			}
		}()
		return heartbeat, results
	}

	done := make(chan interface{})
	time.AfterFunc(10*time.Second, func() { close(done) })
	const timeout = 2 * time.Second
	heartbeat, results := doWork(done, timeout/2)
	for {
		select {
		case _, ok := <-heartbeat:
			if ok == false {
				return
			}
			fmt.Println("pulse")
		case r, ok := <-results:
			if ok == false {
				return
			}
			fmt.Printf("results %v\n", r.Second())
		case <-time.After(timeout):
			fmt.Println("timeout is exceeded")
			return
		}
	}
}

func TestReference(t *testing.T) {
	type S struct {
	}
	a := S{}
	b := a
	println(&a, &b)
}

func TestDebugln(t *testing.T) {
	Debugln("123", "aaa")
}

func TestSliceAlloc(t *testing.T) {
	s := make([][]int, 3)
	s[0] = append(s[0], 1, 2)
	println(s[0])
}

func TestPartition(t *testing.T) {
	contentsByte, err := ioutil.ReadFile("../main/pg-grimm.txt")
	if err != nil {
		panic(err)
	}
	contents := string(contentsByte)

	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	var kva []KeyValue
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	nReduce := 10
	part := partition(kva, nReduce)
	for i, p := range part {
		fmt.Printf("length of partition %v is %v\n", i, len(p))
	}
}

func TestMakeDir(t *testing.T) {
	p := filepath.Join("test", "hello")
	os.RemoveAll(p)
	os.MkdirAll(p, os.ModePerm)
}

func TestRandomize(t *testing.T) {
	fmt.Println(fmt.Sprintf("%v-%d", "wc", time.Now().UnixNano()))
}

func TestMultiSourceMergeSort(t *testing.T) {
	dic := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	generate := func(min, max int) *ByKey {
		rand.Seed(time.Now().UnixNano())
		println(time.Now().UnixNano())
		n := rand.Intn(max-min+1) + min
		kvs := make(ByKey, 0)
		for i := 0; i < n; i++ {
			idx := rand.Intn(len(dic))
			kvs = append(kvs, KeyValue{
				Key:   dic[idx],
				Value: "1",
			})
		}
		sort.Sort(kvs)
		time.Sleep(time.Microsecond)
		return &kvs
	}

	kv1 := generate(2, 3)
	kv2 := generate(2, 3)
	kv3 := generate(2, 3)

	//kv1 := &ByKey{KeyValue{Key: "8", Value: "1"}, KeyValue{Key: "8", Value: "1"}}
	//kv2 := &ByKey{KeyValue{Key: "8", Value: "1"}, KeyValue{Key: "8", Value: "1"}}
	//kv3 := &ByKey{KeyValue{Key: "8", Value: "1"}, KeyValue{Key: "8", Value: "1"}}

	fmt.Printf("kv1: %v\n", *kv1)
	fmt.Printf("kv2: %v\n", *kv2)
	fmt.Printf("kv3: %v\n", *kv3)

	sorted := multiSourceMergeSort([][]KeyValue{*kv1, *kv2, *kv3})
	for _, ele := range sorted {
		fmt.Printf("%v\n", ele)
	}
}

func TestOpenMultipleFiles(t *testing.T) {
	f, _ := os.Open("../main/pg-*")
	content, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	println(string(content))
}
