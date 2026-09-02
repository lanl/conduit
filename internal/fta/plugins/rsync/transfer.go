// Copyright 2026. Triad National Security, LLC. All rights reserved.

package rsync

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/actions"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var rsyncProgressRe = regexp.MustCompile(
	`^\s*([0-9.,]+)\s*([KMGTP]?B?)\s+(\d+)%\s+([0-9.,]+[KMGTP]?B?)`,
)

var rsyncTotalTransferredRe = regexp.MustCompile(
	`^Total transferred file size:\s+([0-9,]+)\s+bytes`,
)

var rsyncFinalBandwidth = regexp.MustCompile(
	`^sent\s+([0-9,]+)\s+bytes\s+received\s+([0-9,]+)\s+bytes\s+([0-9,\.]+)\s+bytes/sec`,
)

var rsyncFinalFiles = regexp.MustCompile(
	`^Number of regular files transferred:\s+([0-9,]+)`,
)

func (p *RsyncPlugin) Transfer(transferID uuid.UUID, pluginData *plugin.PluginData, destInfo proto.DestInfo, action string, options map[string]*anypb.Any, updateTransferProgress plugin.UpdateTransferProgress, updateAction plugin.UpdateAction) *proto.FTAPluginErrors {
	p.log.Debugf("scheduler nodelist: %v", os.Getenv("SLURM_JOB_NODELIST"))
	p.log.Debugf("environ: %+v", os.Environ())

	src := []string{}
	dst := ""

	// go through the pluginData to get sources and destination
	for _, sppi := range pluginData.SourcePluginInfo {
		src = append(src, sppi.TransferPath)
	}

	dst = pluginData.DestinationPluginInfo.TransferPath

	args := src
	args = append(args, dst)

	args = append(args, "--info=progress2") // outputs statistics based on the whole transfer, rather than individual files.
	args = append(args, "--info=name0")     // see how the transfer is doing without scrolling the screen with a lot of names.
	args = append(args, "--stats")          // This tells rsync to print a verbose set of statistics on the file transfer.
	args = append(args, "--links")          // When symlinks are encountered, recreate the symlink on the destination.
	args = append(args, "--perms")          // set the destination permissions to be the same as the source permissions.
	args = append(args, "--times")          // preserve modification times
	args = append(args, "--group")          // preserve group
	args = append(args, "--owner")          // preserve owner
	args = append(args, "--specials")       // preserve special files

	// add recursive flag if it was provided by the user
	if _, ok := options[actions.RecursiveFlag]; ok {
		var rec wrapperspb.BoolValue
		if err := options[actions.RecursiveFlag].UnmarshalTo(&rec); err != nil {
			p.log.Errorf("failed to unmarshal recursive flag: %v", err)
		}

		if rec.GetValue() {
			args = append(args, "--recursive")
		}
	}

	argTest := strings.Join(args, " ")
	s := unsafe.Sizeof(argTest)
	p.log.Debugf("size of args: %d", s)
	realSize := len(argTest) + int(unsafe.Sizeof(argTest))
	p.log.Debugf("real size of args: %d", realSize)

	rsyncConfig := DefaultRsyncPluginConfig()
	err := plugin.GetPluginConfigsFromViper(RsyncPluginKey, &rsyncConfig)
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					PErr:       proto.Error_ERROR_INVALID_CONDUIT_CONFIG,
					ErrMessage: fmt.Sprintf("failed to get rsync config: %v", err),
				},
			},
		}
	}
	rsyncLocation := rsyncConfig.RsyncPath

	cmd := exec.Command(rsyncLocation, args...)

	stdoutp, err := cmd.StdoutPipe()
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("failed to get stdout pipe from command: %v", err),
				},
			},
		}
	}

	stderrp, err := cmd.StderrPipe()
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("failed to get stderr pipe from command: %v", err),
				},
			},
		}
	}

	done := make(chan *proto.FTAPathError)

	stdoutScanner := bufio.NewScanner(stdoutp)
	stdoutScanner.Split(ScanLinesWithCR)
	stderrScanner := bufio.NewScanner(stderrp)
	stderrScanner.Split(bufio.ScanLines)

	stderrText := ""
	nonfatalErrors := ""

	p.log.Infof("rsync command: %v", cmd.Args)

	var wg sync.WaitGroup
	wg.Add(2)

	var stdoutScanErr error
	var stderrScanErr error

	// this go routine will watch the stderr pipe and add it to the stderrText variable
	go func() {
		defer wg.Done()
		// use this to grab the entire stderr pipe
		for stderrScanner.Scan() {
			t := stderrScanner.Text()
			p.log.Errorf("rsync error text: %v", t)
			stderrText = fmt.Sprintf("%v\n%v", stderrText, strings.ToValidUTF8(t, "[invalid-utf8]"))
		}

		if err := stderrScanner.Err(); err != nil {
			stderrScanErr = fmt.Errorf("failed reading rsync stderr: %w", err)
		}
	}()

	// this go routine will watch the stdout pipe
	go func() {
		defer wg.Done()

		for stdoutScanner.Scan() {
			t := stdoutScanner.Text()
			// stdoutText := strings.ToValidUTF8(t, "[invalid-utf8]")
			// p.log.Debugf("rsync output text: %v", t)

			if m := rsyncProgressRe.FindStringSubmatch(t); m != nil {
				if m[2] == "" {
					m[2] = "B"
				}
				dataBytes := strings.ReplaceAll(m[1], ",", "")
				dataSuffix := "B"
				if m[2] != "" {
					dataSuffix = m[2]
				}

				data := strings.TrimSpace(dataBytes + dataSuffix)
				bandwidth := strings.TrimSpace(m[4])

				uErr := updateTransferProgress(&proto.ETCDStatusDetails{
					Data:      data,
					Bandwidth: bandwidth,
				})
				if uErr != nil {
					p.log.Errorf("failed to update rsync transfer progress: %v", uErr)
				}
			}

			if m := rsyncTotalTransferredRe.FindStringSubmatch(t); m != nil {
				dataBytes := strings.ReplaceAll(m[1], ",", "")

				uErr := updateTransferProgress(&proto.ETCDStatusDetails{
					Data: dataBytes + "B",
				})
				if uErr != nil {
					p.log.Errorf("failed to update final rsync transfer progress: %v", uErr)
				}
			}

			if m := rsyncFinalBandwidth.FindStringSubmatch(t); m != nil {
				bandwidth := strings.ReplaceAll(m[3], ",", "")

				uErr := updateTransferProgress(&proto.ETCDStatusDetails{
					Bandwidth: bandwidth + "B",
				})
				if uErr != nil {
					p.log.Errorf("failed to update final rsync transfer progress: %v", uErr)
				}
			}

			if m := rsyncFinalFiles.FindStringSubmatch(t); m != nil {
				files := strings.ReplaceAll(m[1], ",", "")
				filesInt, iErr := strconv.ParseUint(files, 10, 32)
				if iErr != nil {
					p.log.Errorf("failed to convert string[%v] to int: %v", filesInt, iErr)
				} else {
					uErr := updateTransferProgress(&proto.ETCDStatusDetails{
						Files: uint32(filesInt),
					})
					if uErr != nil {
						p.log.Errorf("failed to update final rsync transfer progress: %v", uErr)
					}
				}
			}
		}

		if err := stdoutScanner.Err(); err != nil {
			stdoutScanErr = fmt.Errorf("failed reading rsync stdout: %w", err)
		}

	}()

	// start the rsync command
	if err := cmd.Start(); err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("failed to start rsync command: %v", err),
				},
			},
		}
	}

	// wait for the command to finish in a go routine
	go func() {
		// wait for scanners to finish
		wg.Wait()
		err := cmd.Wait()
		if err != nil {
			done <- &proto.FTAPathError{
				PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
				ErrMessage: fmt.Sprintf("rsync returned non zero exit code: %v", err),
			}
			return
		}

		if stderrScanErr != nil {
			done <- &proto.FTAPathError{
				PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
				ErrMessage: stderrScanErr.Error(),
			}
			return
		}

		if stdoutScanErr != nil {
			done <- &proto.FTAPathError{
				PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
				ErrMessage: stdoutScanErr.Error(),
			}
			return
		}

		done <- &proto.FTAPathError{PErr: proto.Error_ERROR_NONE, ErrMessage: ""}
	}()

	// this will wait for the cmd to finish from the go routine
	errorOccurred := <-done

	warnings := []*proto.FTAPathError{}

	pluginErrors := &proto.FTAPluginErrors{
		Warnings: warnings,
	}

	if errorOccurred.ErrMessage != "" {
		// an error occurred. Format the cmd line in case there are a lot of sources
		cmdOuput := cmd.String()
		if len(cmd.String()) > 5000 {
			cmdOuput = cmd.String()[:2500] + " ...... " + cmd.String()[len(cmd.String())-2500:]
		}
		errMessage := fmt.Sprintf("an error occurred during command[%v]: %+v\n\nrsync stderr output:\n%v", cmdOuput, errorOccurred.ErrMessage, stderrText)
		if p.log.GetLevel() == logrus.DebugLevel {
			errMessage = fmt.Sprintf("an error occurred during command[%v]: %+v\n\ncmd environment: [%+v]\n\nrsync stderr output:\n%v", cmdOuput, errorOccurred.ErrMessage, cmd.Environ(), stderrText)
		}
		if nonfatalErrors != "" {
			errMessage = fmt.Sprintf("%s\n\nrsync nonfatal errors:\n%s", errMessage, nonfatalErrors)
		}

		pluginErrors.Errors = append(pluginErrors.Errors, &proto.FTAPathError{
			PErr:       errorOccurred.PErr,
			ErrMessage: errMessage,
		})
	}

	return pluginErrors
}

func ScanLinesWithCR(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	// Look for either \n or \r
	if i := bytes.IndexAny(data, "\r\n"); i >= 0 {
		// Return the line up to the found character
		return i + 1, data[0:i], nil
	}
	// If at EOF, return the rest of the data
	if atEOF {
		return len(data), data, nil
	}
	// Request more data
	return 0, nil, nil
}
