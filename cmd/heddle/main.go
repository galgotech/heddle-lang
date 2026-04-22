package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.1.0-alpha"

func main() {
	versionFlag := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("Heddle CLI version %s\n", version)
		os.Exit(0)
	}

	fmt.Println("Heddle: The Language for Orchestration Logic")
	fmt.Println("Use -help for usage information.")
}
