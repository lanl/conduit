// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (

	proto "github.com/lanl/conduit/api"
	"strings"
)

// Escape codes for colored server status output
const (
	ColorReset  = "\033[0m"
	ColorBold   = "\033[1m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorBlue   = "\033[34m"
	ColorYellow = "\033[33m"
)

func ColorizeServerState(state proto.ServerState) string {
	var msgColor string
	switch state {
	case proto.ServerState_SERVER_RUNNING:
		msgColor = ColorGreen
	case proto.ServerState_SERVER_STARTING,
		proto.ServerState_SERVER_STOPPING,
		proto.ServerState_SERVER_DRAIN_INIT,
		proto.ServerState_SERVER_DRAINING,
		proto.ServerState_SERVER_UNKNOWN:
		msgColor = ColorYellow
	case proto.ServerState_SERVER_ERROR:
		msgColor = ColorRed
	case proto.ServerState_SERVER_STOPPED:
		msgColor = ColorReset
	}

	return msgColor
}

func CalculateSeparatorWidth(termWidth int, width int) string {
	var separator string
	if termWidth >= width {
		separator = strings.Repeat("─", width)
	} else {
		separator = strings.Repeat("─", termWidth)
	}
	return separator
}
