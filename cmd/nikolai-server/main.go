package main

import (
	"flag"
	"os"

	"github.com/tidwall/nikolai"
)

func main() {
	var addr, dir, join string

	// command-line parameters
	flag.StringVar(&addr, "addr", ":7480", "server bind address")
	flag.StringVar(&dir, "dir", "data", "data directory")
	flag.StringVar(&join, "join", "", "join address, if any")
	flag.Parse()

	logger := nikolai.NewLogger(os.Stderr)
	// start server
	n, err := nikolai.Open(dir, addr, join, &nikolai.Options{Logger: logger})
	if err != nil {
		// do not output the err, because open already did
		os.Exit(1)
	}
	defer n.Close()

	select {}
}
