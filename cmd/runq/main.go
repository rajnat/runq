package main

import (
	"fmt"
	"os"

	"github.com/eswar/runq/internal/cli"
)

func main() {
	app := cli.New(cli.LoadConfigFromEnv(), os.Stdout, os.Stderr)
	if err := app.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
