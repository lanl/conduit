// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"context"
	"os"
	"time"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/logger"
	"github.com/sirupsen/logrus"
)

// FTAInit creates a new logger, extracts certificate information from stdin, creates an etcd manager, and retrieves the node list from the environment.
func FTAInit(debug bool) (_ *logger.ConduitLogger, _ *proto.TransferDetails, _ *FTAClient, nodeList string) {
	log := logger.NewConduitLogger(logrus.InfoLevel, "")
	if debug {
		log = logger.NewConduitLogger(logrus.DebugLevel, "")
	}

	socketPath := os.Getenv(defaults.FTASocketEnvVar)
	if socketPath == "" {
		log.Fatalf("%s environment variable is not set", defaults.FTASocketEnvVar)
	}

	client, err := NewFTAClient(socketPath)
	if err != nil {
		log.Fatalf("failed to create FTA runner client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transfer, err := client.GetTransfer(ctx)
	if err != nil {
		_ = client.Close()

		log.Fatalf("failed to get transfer from runner: %v", err)
	}

	nodeList = os.Getenv("SLURM_JOB_NODELIST")

	return log, transfer, client, nodeList
}
