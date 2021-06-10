package mr

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type chunk struct {
	data     []byte
	filename string
}

type chunks []chunk

var (
	DebugMode = false
	logMutex  = &sync.Mutex{}
)

// getFileSize returns the size in byte of the specified filename
func getFileSize(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %v, error %v", filename, err)
		return 0, err
	}
	fs, err := file.Stat()
	if err != nil {
		log.Fatalf("cannot get stat of file %v, error %v", filename, err)
		return 0, err
	}
	return fs.Size(), nil
}

func containsKey(m map[interface{}]interface{}, k interface{}) bool {
	_, ok := m[k]
	return ok
}

func Infof(format string, a ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	format = fmt.Sprintf("[INFO] %v", format)
	log.Printf(format, a...)
}

func Infoln(a ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if len(a) == 0 {
		log.Println()
		return
	}
	s := "[INFO]"
	for _, i := range a {
		s = fmt.Sprintf("%v %v", s, i)
	}
	log.Println(s)
}

func Errorf(format string, a ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	format = fmt.Sprintf("[ERROR] %v", format)
	log.Printf(format, a...)
}

func Errorln(a ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if len(a) == 0 {
		log.Println()
		return
	}
	s := "[ERROR]"
	for _, i := range a {
		s = fmt.Sprintf("%v %v", s, i)
	}
	log.Println(s)
}

func Debugf(format string, a ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if DebugMode {
		format = fmt.Sprintf("[DEBUG] %v", format)
		log.Printf(format, a...)
	}
}

func Debugln(a ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if DebugMode {
		if len(a) == 0 {
			log.Println()
			return
		}
		s := "[DEBUG]"
		for _, i := range a {
			s = fmt.Sprintf("%v %v", s, i)
		}
		log.Println(s)
	}
}
