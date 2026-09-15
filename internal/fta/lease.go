// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"context"
	"time"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/emptypb"
)

func updateTransferExpiry(ctx context.Context, client *FTAClient) {
	expiryTicker := time.NewTicker(viper.GetDuration(defaults.ConfigExpiryIntervalKey))
	logrus.Debugf("starting UpdateExpiry")

	hCtx := context.Context(context.Background())
	resp, err := client.api.Heartbeat(hCtx, &emptypb.Empty{})
	handleExpiryResponse(resp, err)

	for {
		select {
		case <-expiryTicker.C:
			resp, err := client.api.Heartbeat(hCtx, &emptypb.Empty{})
			handleExpiryResponse(resp, err)

		case <-ctx.Done():
			expiryTicker.Stop()
			logrus.Warnf("UpdateExpiry was stopped")
			return
		}
	}
}

func handleExpiryResponse(resp *proto.FTAHeartbeatResponse, err error) {
	if err != nil {
		logrus.Errorf("error committing updated expiry to etcd: %v", err)
		return
	}
	if resp == nil {
		logrus.Error("heartbeat returned nil response")
		return
	}

	if resp.GetTransferError() == proto.Error_ERROR_ABORTED {
		logrus.Fatal("The Transfer was aborted, stopping...")
	}
	if resp.GetTransferError() != proto.Error_ERROR_NONE {
		logrus.Fatalf("The Transfer is in an error state[%s], stopping...", resp.GetTransferError())
	}
	if !resp.GetSuccessful() {
		logrus.Fatal("updating the transfer expiry was unsuccessful")
	}

	if resp.GetNewExpiry() != nil {
		logrus.Debugf("successfully updated expiry: %s", resp.GetNewExpiry().AsTime().Format(time.RFC3339))
	}
}
