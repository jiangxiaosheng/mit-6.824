package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"../mr"
	"context"
	"flag"
)
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	flag.BoolVar(&mr.DebugMode, "debug", false, "used for debugging")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mr.MakeMaster(ctx, os.Args[1:], 10)
}
