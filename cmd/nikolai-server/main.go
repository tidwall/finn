package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/tidwall/nikolai"
)

func main() {
	var addr, dir, join, durability, consistency string
	var quiet, verbose, veryVerbose bool

	// command-line parameters
	flag.StringVar(&addr, "addr", ":7480", "server bind address")
	flag.StringVar(&dir, "dir", "data", "data directory")
	flag.StringVar(&join, "join", "", "join address, if any")
	flag.StringVar(&durability, "durability", "medium", "write durability level: low,medium,high")
	flag.StringVar(&consistency, "consistency", "medium", "read consistency level: low,medium,high")
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging")
	flag.BoolVar(&veryVerbose, "vv", false, "Enable very verbose logging")
	flag.BoolVar(&quiet, "q", false, "Quiet logging. Totally silent")
	flag.Parse()

	var output io.Writer
	if quiet {
		output = ioutil.Discard
	} else {
		output = os.Stderr
	}

	var logger *nikolai.Logger
	logger = nikolai.NewLogger(output)
	if veryVerbose {
		logger.SetAccept("$!*#")
	} else if verbose {
		logger.SetAccept("!*#")
	} else {
		logger.SetAccept("!*")
	}
	opts := &nikolai.Options{
		Logger:      logger,
		Durability:  levelArg(durability, "durability"),
		Consistency: levelArg(consistency, "consistency"),
	}

	// start server
	n, err := nikolai.Open(dir, addr, join, opts)
	if err != nil {
		// do not output the err because Open() already did
		os.Exit(1)
	}
	defer n.Close()

	select {}
}

func levelArg(arg string, name string) nikolai.Level {
	switch strings.ToLower(arg) {
	default:
		fmt.Fprintf(os.Stderr, "invalid %s specified\n", name)
		os.Exit(1)
		return nikolai.High
	case "low":
		return nikolai.Low
	case "med", "medium":
		return nikolai.Medium
	case "high":
		return nikolai.High
	}
}
