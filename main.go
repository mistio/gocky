package main

import (
	"flag"
	"os"
	"os/signal"

	log "github.com/golang/glog"

	"github.com/mistio/gocky/relay"
)

var (
	configFile = flag.String("config", "", "Configuration file to use")
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	if *configFile == "" {
		log.Fatal("Missing configuration file")
	}

	cfg, err := relay.LoadConfigFile(*configFile)
	if err != nil {
		log.Error("Problem loading config file:", err)
	}

	r, err := relay.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		r.Stop()
	}()

	log.Info("Starting relays...")
	r.Run()
}
