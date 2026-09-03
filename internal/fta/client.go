// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"context"
	"errors"
	"fmt"

	"github.com/lanl/conduit/api"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type FTAClient struct {
	conn *grpc.ClientConn
	api  proto.ConduitFTAApiClient
}

func NewFTAClient(socketPath string) (*FTAClient, error) {
	if socketPath == "" {
		return nil, fmt.Errorf("FTA socket path is empty")
	}

	target := "unix://" + socketPath

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create FTA gRPC client for socket[%s]: %w",
			socketPath,
			err,
		)
	}

	return &FTAClient{
		conn: conn,
		api:  proto.NewConduitFTAApiClient(conn),
	}, nil
}

func (c *FTAClient) Close() error {
	return c.conn.Close()
}

func (c *FTAClient) GetTransfer(ctx context.Context) (*proto.TransferDetails, error) {
	transfer, err := c.api.GetTransfer(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get transfer from runner: %w", err)
	}

	return transfer, nil
}

func (c *FTAClient) StartPlugin(ctx context.Context) (_ proto.Error, _ error) {
	go updateTransferExpiry(ctx, c)

	resp, err := c.api.Start(ctx, &emptypb.Empty{})
	if err != nil {
		return resp.GetError(), errors.New(resp.GetErrorMessage())
	}

	return proto.Error_ERROR_NONE, nil
}

func (c *FTAClient) CompletePlugin(ctx context.Context, command proto.SchedulerCommand, pluginData *plugin.PluginData, destInfo proto.DestInfo, pluginErrors *proto.FTAPluginErrors) (proto.Error, error) {
	var pluginDataBytes []byte
	if pluginData != nil {
		bb, err := plugin.EncodePluginData(pluginData)
		if err != nil {
			return api.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to encode plugin data: %v", err)
		}
		pluginDataBytes = bb.Bytes()
	}

	var leases *proto.Leases

	if command == proto.SchedulerCommand_VALIDATION {
		sourceLeases := []string{}
		destinationLeases := []string{}

		if pluginData != nil {
			for _, s := range pluginData.SourcePluginInfo {
				sourceLeases = append(sourceLeases, s.ResolvedFTAPath)
			}

			for _, d := range pluginData.DestinationsPluginInfo {
				destinationLeases = append(destinationLeases, d.ResolvedFTAPath)
			}
		}

		leases = &proto.Leases{
			Source:      sourceLeases,
			Destination: destinationLeases,
		}
	}

	resp, err := c.api.Complete(ctx, &api.FTACompleteRequest{
		PluginData:   pluginDataBytes,
		Leases:       leases,
		DestInfo:     destInfo,
		PluginErrors: pluginErrors,
	})
	if err != nil {
		return resp.GetError(), errors.New(resp.GetErrorMessage())
	}

	return proto.Error_ERROR_NONE, nil
}

func (c *FTAClient) FailPlugin(ctx context.Context, pluginData *plugin.PluginData, destInfo proto.DestInfo, pluginErrors *proto.FTAPluginErrors) (proto.Error, error) {
	var pluginDataBytes []byte
	if pluginData != nil {
		bb, err := plugin.EncodePluginData(pluginData)
		if err != nil {
			return api.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to encode plugin data: %v", err)
		}
		pluginDataBytes = bb.Bytes()
	}

	resp, err := c.api.Fail(ctx, &api.FTAFailRequest{
		PluginData:   pluginDataBytes,
		DestInfo:     destInfo,
		PluginErrors: pluginErrors,
	})
	if err != nil {
		return resp.GetError(), err
	}

	return proto.Error_ERROR_NONE, nil
}
