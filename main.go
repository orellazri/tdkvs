package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/orellazri/tdkvs/master"
	"github.com/orellazri/tdkvs/volume"
)

func main() {
	masterCmd := flag.NewFlagSet("master", flag.ExitOnError)
	masterPort := masterCmd.Int("port", 3000, "port for the master server")

	volumeCmd := flag.NewFlagSet("volume", flag.ExitOnError)
	volumePort := volumeCmd.Int("port", 3001, "port for the volume server")

	if len(os.Args) < 2 {
		fmt.Println("expected `master` or `volume` subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "master":
		masterCmd.Parse(os.Args[2:])
		master.Start(*masterPort)
	case "volume":
		volumeCmd.Parse(os.Args[2:])
		fmt.Println("Volume!")
		fmt.Println("	port: ", *volumePort)
		volume.Start(*masterPort)
	default:
		fmt.Println("expected `master` or `volume` subcommands")
		os.Exit(1)
	}
}
