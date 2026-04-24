package api

import "testing"

func TestRegisterWorkerRequestValidateRejectsBlankOnlyQueues(t *testing.T) {
	req := RegisterWorkerRequest{
		Name:           "worker-a",
		Queues:         []string{"   ", "\t"},
		MaxConcurrency: 1,
	}

	if err := req.Validate(); err == nil {
		t.Fatal("expected validation error for blank-only queues")
	}
}
