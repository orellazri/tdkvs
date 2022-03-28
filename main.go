package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/orellazri/tdkvs/master"
	"github.com/orellazri/tdkvs/utils"
	"github.com/orellazri/tdkvs/volume"
	"gopkg.in/yaml.v2"
)

func main() {
	masterCmd := flag.NewFlagSet("master", flag.ExitOnError)
	masterPort := masterCmd.Int("port", 3000, "port for the master server")
	masterConfig := masterCmd.String("config", "", "path to config file")

	volumeCmd := flag.NewFlagSet("volume", flag.ExitOnError)
	volumePort := volumeCmd.Int("port", 3001, "port for the volume server")

	if len(os.Args) < 2 {
		fmt.Println("expected `master` or `volume` subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "master":
		masterCmd.Parse(os.Args[2:])

		if *masterConfig == "" {
			fmt.Println("a config file is required")
			os.Exit(1)
		}

		config := utils.Config{}
		data, err := os.ReadFile(*masterConfig)
		utils.AbortOnError(err)
		err = yaml.Unmarshal(data, &config)
		utils.AbortOnError(err)

		master.Start(*masterPort, &config)
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
