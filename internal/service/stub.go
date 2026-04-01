package service

import (
	"log"
	"time"
)

func RunStub(logger *log.Logger, component string, interval time.Duration) {
	logger.Printf("starting %s loop with interval %s", component, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		logger.Printf("%s tick", component)
	}
}
