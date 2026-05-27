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
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/sirupsen/logrus"
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

func (p *RsyncPlugin) Transfer(transferID uuid.UUID, pluginData *plugin.PluginData, destInfo proto.DestInfo, action proto.Action, updateTransferProgress plugin.UpdateTransferProgress, updateAction plugin.UpdateAction) plugin.PluginErrors {
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
	// args = append(args, "-v")
	// args = append(args, "-P")

	args = append(args, "--info=progress2")
	args = append(args, "--info=name0")
	args = append(args, "--stats")

	if action == proto.Action_RECURSIVE_COPY || action == proto.Action_RECURSIVE_MOVE {
		args = append(args, "-r")
	}

	argTest := strings.Join(args, " ")
	s := unsafe.Sizeof(argTest)
	p.log.Debugf("size of args: %d", s)
	realSize := len(argTest) + int(unsafe.Sizeof(argTest))
	p.log.Debugf("real size of args: %d", realSize)

	rsyncConfig := &ViperRsyncPluginConfig{}
	err := plugin.GetPluginConfigsFromViper(RsyncPluginKey, rsyncConfig)
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_INVALID_CONDUIT_CONFIG,
					ErrMessage: fmt.Errorf("failed to get rsync config: %v", err),
				},
			},
		}
	}
	rsyncLocation := rsyncConfig.RsyncPath

	cmd := exec.Command(rsyncLocation, args...)

	stdoutp, err := cmd.StdoutPipe()
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Errorf("failed to get stdout pipe from command: %v", err),
				},
			},
		}
	}

	stderrp, err := cmd.StderrPipe()
	if err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Errorf("failed to get stderr pipe from command: %v", err),
				},
			},
		}
	}

	done := make(chan *plugin.FTAPathError)

	stdoutScanner := bufio.NewScanner(stdoutp)
	stdoutScanner.Split(ScanLinesWithCR)
	stderrScanner := bufio.NewScanner(stderrp)
	stderrScanner.Split(bufio.ScanLines)

	stderrText := ""
	nonfatalErrors := ""

	p.log.Infof("rsync command: %v", cmd.Args)

	var wg sync.WaitGroup
	wg.Add(2)

	// this go routine will watch the stderr pipe and add it to the stderrText variable
	go func() {
		defer wg.Done()
		// use this to grab the entire stderr pipe
		for stderrScanner.Scan() {
			t := stderrScanner.Text()
			p.log.Errorf("rsync error text: %v", t)
			stderrText = fmt.Sprintf("%v\n%v", stderrText, strings.ToValidUTF8(t, "[invalid-utf8]"))
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

				uErr := updateTransferProgress(proto.ETCDStatusDetails{
					Data:      data,
					Bandwidth: bandwidth,
				})
				if uErr != nil {
					p.log.Errorf("failed to update rsync transfer progress: %v", uErr)
				}
			}

			if m := rsyncTotalTransferredRe.FindStringSubmatch(t); m != nil {
				dataBytes := strings.ReplaceAll(m[1], ",", "")

				uErr := updateTransferProgress(proto.ETCDStatusDetails{
					Data: dataBytes + "B",
				})
				if uErr != nil {
					p.log.Errorf("failed to update final rsync transfer progress: %v", uErr)
				}
			}

			if m := rsyncFinalBandwidth.FindStringSubmatch(t); m != nil {
				bandwidth := strings.ReplaceAll(m[3], ",", "")

				uErr := updateTransferProgress(proto.ETCDStatusDetails{
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
					uErr := updateTransferProgress(proto.ETCDStatusDetails{
						Files: uint32(filesInt),
					})
					if uErr != nil {
						p.log.Errorf("failed to update final rsync transfer progress: %v", uErr)
					}
				}

			}

		}
	}()

	// start the rsync command
	if err := cmd.Start(); err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Errorf("failed to start rsync command: %v", err),
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
			done <- &plugin.FTAPathError{PErr: proto.Error_ERROR_FTA_PLUGIN_FAILED, ErrMessage: fmt.Errorf("rsync returned non zero exit code: %v", err)}
			return
		}
		done <- &plugin.FTAPathError{PErr: proto.Error_ERROR_NONE, ErrMessage: nil}
	}()

	// this will wait for the cmd to finish from the go routine
	errorOccurred := <-done

	warnings := []*plugin.FTAPathError{}

	pluginErrors := plugin.PluginErrors{
		Warnings: warnings,
	}

	if errorOccurred.ErrMessage != nil {
		// an error occurred. Format the cmd line in case there are a lot of sources
		cmdOuput := cmd.String()
		if len(cmd.String()) > 5000 {
			cmdOuput = cmd.String()[:2500] + " ...... " + cmd.String()[len(cmd.String())-2500:]
		}
		errMessage := fmt.Errorf("an error occurred during command[%v]: %+v\n\nrsync stderr output:\n%v", cmdOuput, errorOccurred.ErrMessage, stderrText)
		if p.log.GetLevel() == logrus.DebugLevel {
			errMessage = fmt.Errorf("an error occurred during command[%v]: %+v\n\ncmd environment: [%+v]\n\nrsync stderr output:\n%v", cmdOuput, errorOccurred.ErrMessage, cmd.Environ(), stderrText)
		}
		if nonfatalErrors != "" {
			errMessage = fmt.Errorf("%s\n\nrsync nonfatal errors:\n%s", errMessage, nonfatalErrors)
		}

		pluginErrors.Errors = append(pluginErrors.Errors, &plugin.FTAPathError{
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
