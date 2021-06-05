package mr

import (
	"fmt"
	"testing"
)

func TestGetFreePort(t *testing.T) {
	port := getFreePort()
	fmt.Println(port)
}
