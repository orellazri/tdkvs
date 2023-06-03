package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/orellazri/tdkvs/internal/master"
	"github.com/orellazri/tdkvs/internal/utils"
	"github.com/orellazri/tdkvs/internal/volume"
	"gopkg.in/yaml.v2"
)

func main() {
	masterCmd := flag.NewFlagSet("master", flag.ExitOnError)
	masterConfigPath := masterCmd.String("config", "", "path to config file for the master server")
	masterDeleteVolume := masterCmd.Int("delete", -1, "delete a volume server")

	volumeCmd := flag.NewFlagSet("volume", flag.ExitOnError)
	volumeConfigPath := volumeCmd.String("config", "", "path to config file for the volume server")

	if len(os.Args) < 2 {
		fmt.Println("expected `master` or `volume` subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "master":
		masterCmd.Parse(os.Args[2:])

		if *masterConfigPath == "" {
			fmt.Println("config file is required. specify a path with -config")
			os.Exit(1)
		}

		config := &master.Config{}
		data, err := os.ReadFile(*masterConfigPath)
		utils.AbortOnError(err)
		err = yaml.Unmarshal(data, &config)
		if err != nil {
			fmt.Println("The config yaml file specified is invalid!")
			os.Exit(1)
		}
		config.DeleteVolume = *masterDeleteVolume

		// Check if delete volume flag is set
		if *masterDeleteVolume != -1 {
			master.Start(config, master.DeleteVolume)
		} else {
			master.Start(config, master.Normal)
		}
	case "volume":
		volumeCmd.Parse(os.Args[2:])

		if *volumeConfigPath == "" {
			fmt.Println("config file is required. specify a path with -config")
			os.Exit(1)
		}
		config := &volume.Config{}
		data, err := os.ReadFile(*volumeConfigPath)
		utils.AbortOnError(err)
		err = yaml.Unmarshal(data, &config)
		if err != nil {
			fmt.Println("The config yaml file specified is invalid!")
			os.Exit(1)
		}

		volume.Start(config)
	default:
		fmt.Println("expected `master` or `volume` subcommands")
		os.Exit(1)
	}
}
