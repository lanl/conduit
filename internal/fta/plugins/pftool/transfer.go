// Copyright 2026. Triad National Security, LLC. All rights reserved.

package pftool

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/actions"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (p *PftoolPlugin) Transfer(transferID uuid.UUID, pluginData *plugin.PluginData, destInfo proto.DestInfo, action string, options map[string]*anypb.Any, updateTransferProgress plugin.UpdateTransferProgress, updateAction plugin.UpdateAction) plugin.PluginErrors {
	updateTransferProgress(proto.ETCDStatusDetails{
		PluginStatus: "starting pftool",
	})

	ulimitOut, err := exec.Command("/bin/sh", "-c", "ulimit -a").Output()
	if err != nil {
		p.log.Errorf("failed to call ulimit command: %v", err)
	}
	p.log.Debugf(string(ulimitOut))

	p.log.Debugf("scheduler nodelist: %v", os.Getenv("SLURM_JOB_NODELIST"))
	p.log.Debugf("environ: %+v", os.Environ())

	// increase stack size
	// this is required by pftool
	var rLimit syscall.Rlimit
	err = syscall.Getrlimit(syscall.RLIMIT_STACK, &rLimit)
	if err != nil {
		p.log.Errorf("Error Getting Rlimit ", err)
	}
	p.log.Debugf("initial rlimit: %+v", rLimit)

	rLimit.Cur = rLimit.Max
	err = syscall.Setrlimit(syscall.RLIMIT_STACK, &rLimit)
	if err != nil {
		p.log.Errorf("Error Setting Rlimit ", err)
	}
	err = syscall.Getrlimit(syscall.RLIMIT_STACK, &rLimit)
	if err != nil {
		p.log.Errorf("Error Getting Rlimit ", err)
	}
	p.log.Debugf("final rlimit: %+v", rLimit)

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
	// -s will tell pftool to not follow symlinks
	args = append(args, "-s")
	// args = append(args, "--debug")

	// add recursive flag if it was provided by the user
	if _, ok := options[actions.RecursiveFlag]; ok {
		var rec wrapperspb.BoolValue
		if err := options[actions.RecursiveFlag].UnmarshalTo(&rec); err != nil {
			p.log.Errorf("failed to unmarshal recursive flag: %v", err)
		}

		if rec.GetValue() {
			args = append(args, "-R")
		}
	}

	argTest := strings.Join(args, " ")
	s := unsafe.Sizeof(argTest)
	p.log.Debugf("size of args: %d", s)
	realSize := len(argTest) + int(unsafe.Sizeof(argTest))
	p.log.Debugf("real size of args: %d", realSize)

	cmdContext, cmdCancel := context.WithCancelCause(context.Background())

	pftoolConfig := &ViperPftoolPluginConfig{}
	err = plugin.GetPluginConfigsFromViper(PftoolPluginKey, pftoolConfig)
	if err != nil {
		cmdCancel(fmt.Errorf("failed to get pftool config: %v", err))
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_INVALID_CONDUIT_CONFIG,
					ErrMessage: fmt.Errorf("failed to get pftool config: %v", err),
				},
			},
		}
	}
	pfcpLocation := pftoolConfig.PfcpPath

	cmd := exec.CommandContext(cmdContext, pfcpLocation, args...)

	stdoutp, err := cmd.StdoutPipe()
	if err != nil {
		cmdCancel(fmt.Errorf("failed to get stdout pipe from command: %v", err))
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_PFTOOL_FAILED,
					ErrMessage: fmt.Errorf("failed to get stdout pipe from command: %v", err),
				},
			},
		}
	}

	stderrp, err := cmd.StderrPipe()
	if err != nil {
		cmdCancel(fmt.Errorf("failed to get stderr pipe from command: %v", err))
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_PFTOOL_FAILED,
					ErrMessage: fmt.Errorf("failed to get stderr pipe from command: %v", err),
				},
			},
		}
	}

	// set home dir of this user from passwd
	currUser, err := user.Current()
	if err != nil {
		cmdCancel(fmt.Errorf("failed to get current user: %v", err))
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_CONDUIT_INTERNAL,
					ErrMessage: fmt.Errorf("failed to get current user: %v", err),
				},
			},
		}
	}
	p.log.Debugf("user: %+v %v", currUser, err)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("HOME=%s", currUser.HomeDir))

	done := make(chan *plugin.FTAPathError)
	fileChunksChan := make(chan uint32, 10)

	stdoutScanner := bufio.NewScanner(stdoutp)
	stdoutScanner.Split(bufio.ScanLines)
	stderrScanner := bufio.NewScanner(stderrp)
	stderrScanner.Split(bufio.ScanLines)

	stderrText := ""
	stdoutText := ""
	nonfatalErrors := ""
	detectedNoMovePath := ""

	p.log.Infof("pfcp command: %v", cmd.Args)

	go func() {
		pftoolTimeout := pftoolConfig.TimeoutHours

		if pftoolTimeout == 0 {
			p.log.Warnf("pftool timeout was set to 0 in the config using default value: %v", DefaultPftoolTimeoutHours)
			pftoolTimeout = DefaultPftoolTimeoutHours
		}

		timer := time.NewTimer(time.Duration(pftoolTimeout * float64(time.Hour)))
		defer timer.Stop()

		currFileChunks := uint32(0)

		for {
			select {
			case fc, ok := <-fileChunksChan:
				if !ok {
					return
				}

				// if we get a new value for file chunks, reset the timer
				if fc != currFileChunks {
					timer.Reset(time.Duration(pftoolTimeout * float64(time.Hour)))
				}
				currFileChunks = fc

			case <-timer.C:
				e := fmt.Errorf("No progress detected from pftool in the last %v hour(s). Killing pftool", pftoolTimeout)
				p.log.Error(e)
				cmdCancel(e)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// this go routine will watch the stderr pipe and add it to the stderrText variable
	go func() {
		defer wg.Done()
		// use this to grab the entire stderr pipe
		for stderrScanner.Scan() {
			t := stderrScanner.Text()
			p.log.Errorf("command error text: %v", t)
			stderrText = fmt.Sprintf("%v\n%v", stderrText, strings.ToValidUTF8(t, "[invalid-utf8]"))
		}
	}()

	// this go routine will watch the stdout pipe
	go func() {
		defer wg.Done()
		for stdoutScanner.Scan() {
			t := stdoutScanner.Text()
			stdoutText = fmt.Sprintf("%v\n%v", stdoutText, strings.ToValidUTF8(t, "[invalid-utf8]"))
			_, after, found := strings.Cut(strings.ToValidUTF8(t, "[invalid-utf8]"), ConduitPrefix)
			// only react to messages with the conduit prefix
			if found {
				// parse the message
				b := []byte(strings.TrimSpace(after))
				pmt, err := getPFToolMessageType(b)
				if err != nil {
					done <- &plugin.FTAPathError{PErr: proto.Error_ERROR_PFTOOL_FAILED, ErrMessage: fmt.Errorf("failed to parse pftool message type: %v", err)}
					return
				}

				var fErr error

				switch pmt {
				case PFTMessageType_ERROR:
					pm, err := parsePFTError(b)
					if err != nil {
						fErr = fmt.Errorf("failed to parse pftool error message: %v %v", string(b), err)
						break
					}
					if pm.Class == "NONFATAL" {
						nfErr := fmt.Errorf("pftool nonfatal error message: class: %v errno: %v message: %v origin: %v", pm.Class, pm.Errno, pm.Message, pm.Origin)
						p.log.Error(nfErr)
						nonfatalErrors = fmt.Sprintf("%v\n%v", nonfatalErrors, nfErr)
					} else {
						fErr = fmt.Errorf("pftool error message: class: %v errno: %v message: %v origin: %v", pm.Class, pm.Errno, pm.Message, pm.Origin)
						break
					}
				case PFTMessageType_ACCUM:
					pm, err := parsePFTAccum(b)
					if err != nil {
						fErr = fmt.Errorf("failed to parse pftool accum message: %v %v", string(b), err)
						break
					}
					p.log.Infof("pftool accum message: dataFinished: %v bandwidth: %v filesChunks: %v", pm.DataFinished, pm.Bandwidth, pm.FilesChunks)
					uErr := updateTransferProgress(proto.ETCDStatusDetails{
						Data:        pm.DataFinished,
						Bandwidth:   pm.Bandwidth,
						FilesChunks: uint32(pm.FilesChunks),
					})
					if uErr != nil {
						p.log.Errorf("failed to update etcd with progress: %v", uErr)
					}
					fileChunksChan <- uint32(pm.FilesChunks)
				case PFTMessageType_NOMOVE:
					pm, err := parsePFTNoMove(b)
					if err != nil {
						fErr = fmt.Errorf("failed to parse pftool nomove message: %v %v", string(b), err)
						break
					}
					p.log.Infof("pftool NOMOVE message: path: %v", pm.Path)
					detectedNoMovePath = pm.Path
				case PFTMessageType_HEADER:
					pm, perr := parsePFTHeader(b)
					if perr != nil {
						fErr = fmt.Errorf("failed to parse pftool header message: %v %v", string(b), err)
						break
					}
					p.log.Infof("pftool header message: dstfs: %v srcfs: %v", pm.DestinationFS, pm.SourceFS)
				case PFTMessageType_FOOTER:
					pm, perr := parsePFTFooter(b)
					if perr != nil {
						fErr = fmt.Errorf("failed to parse pftool footer message: %v %v", string(b), err)
						break
					}
					p.log.Infof("pftool footer message: data: %v bandwidth: %v filesChunks: %v directories: %v files: %v", pm.Data, pm.Bandwidth, pm.FilesChunks, pm.Directories, pm.Files)
					uErr := updateTransferProgress(proto.ETCDStatusDetails{
						Data:        pm.Data,
						Bandwidth:   pm.Bandwidth,
						FilesChunks: uint32(pm.FilesChunks),
						Directories: uint32(pm.Directories),
						Files:       uint32(pm.Files),
					})
					if uErr != nil {
						p.log.Errorf("failed to update etcd with progress: %v", uErr)
					}
					fileChunksChan <- uint32(pm.FilesChunks)
				default:
					p.log.Errorf("unrecognized pftool message type: %v", pmt)
					p.log.Debugf("conduit message text: %v", t)
				}

				if fErr != nil {
					done <- &plugin.FTAPathError{PErr: proto.Error_ERROR_PFTOOL_FAILED, ErrMessage: fErr}
					return
				}
			} else {
				p.log.Debugf("pfcp output text: %v", t)
			}
		}
	}()

	// start the pftool command
	if err := cmd.Start(); err != nil {
		return plugin.PluginErrors{
			Errors: []*plugin.FTAPathError{
				{
					PErr:       proto.Error_ERROR_PFTOOL_FAILED,
					ErrMessage: fmt.Errorf("failed to start pftool command: %v", err),
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
			done <- &plugin.FTAPathError{PErr: proto.Error_ERROR_PFTOOL_FAILED, ErrMessage: fmt.Errorf("pftool returned non zero exit code: %v %v", err, context.Cause(cmdContext))}
			return
		}
		done <- &plugin.FTAPathError{PErr: proto.Error_ERROR_NONE, ErrMessage: nil}
	}()

	updateTransferProgress(proto.ETCDStatusDetails{
		PluginStatus: "pftool started",
	})

	// this will wait for the cmd to finish from the go routine
	errorOccurred := <-done

	warnings := []*plugin.FTAPathError{}

	// check if pftool printed a NOMOVE message
	if detectedNoMovePath != "" {
		// sometimes pftool will send a message to conduit indicating that it should do a copy instead of a move.
		if action == actions.Action_MOVE {
			err := updateAction(action, actions.Action_COPY)
			if err != nil {
				if errorOccurred.ErrMessage == nil {
					errorOccurred = &plugin.FTAPathError{PErr: proto.Error_ERROR_ETCD_CONNECTION, ErrMessage: fmt.Errorf("failed to update transfer action to %v: %v", actions.Action_COPY, err)}
				} else {
					warnings = append(warnings, &plugin.FTAPathError{
						ErrMessage: fmt.Errorf("detected write protected children[%v] but failed to update transfer action to copy: %v", detectedNoMovePath, err),
					})
				}
			} else {
				warnings = append(warnings, &plugin.FTAPathError{
					ErrMessage: fmt.Errorf("detected write protected children[%v]. changed transfer action to COPY", detectedNoMovePath),
				})
			}
		} else {
			p.log.Warnf("ignoring NOMOVE message because we are in action[%v]: %v", action, detectedNoMovePath)
		}
	}

	pluginErrors := plugin.PluginErrors{
		Warnings: warnings,
	}

	if errorOccurred.ErrMessage != nil {
		// an error occurred. Format the cmd line in case there are a lot of sources
		cmdOuput := cmd.String()
		if len(cmd.String()) > 5000 {
			cmdOuput = cmd.String()[:2500] + " ...... " + cmd.String()[len(cmd.String())-2500:]
		}
		errMessage := fmt.Errorf("an error occurred during command[%v]: %+v\n\npftool stderr output:\n%v", cmdOuput, errorOccurred.ErrMessage, stderrText)
		if p.log.GetLevel() == logrus.DebugLevel {
			errMessage = fmt.Errorf("an error occurred during command[%v]: %+v\n\ncmd environment: [%+v]\n\npftool stderr output:\n%v\n\npftool stdout output:\n%v", cmdOuput, errorOccurred.ErrMessage, cmd.Environ(), stderrText, stdoutText)
		}
		if nonfatalErrors != "" {
			errMessage = fmt.Errorf("%s\n\npftool nonfatal errors:\n%s", errMessage, nonfatalErrors)
		}

		pluginErrors.Errors = append(pluginErrors.Errors, &plugin.FTAPathError{
			PErr:       errorOccurred.PErr,
			ErrMessage: errMessage,
		})
	} else {
		updateTransferProgress(proto.ETCDStatusDetails{
			PluginStatus: "pftool complete",
		})
	}

	return pluginErrors
}
