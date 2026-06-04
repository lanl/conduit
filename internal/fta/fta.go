// Copyright 2026. Triad National Security, LLC. All rights reserved.

package fta

import (
	"bufio"
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"os"

	brotli "github.com/andybalholm/brotli"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/etcd"
	"github.com/lanl/conduit/internal/etcd/util"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// FTAInit creates a new logger, extracts certificate information from stdin, creates an etcd manager, and retrieves the node list from the environment.
func FTAInit(debug bool) (_ *logger.ConduitLogger, _ proto.IncompleteTransfer, _ *etcd.ETCDManager, nodeList string) {
	log := logger.NewConduitLogger(logrus.InfoLevel, "")
	if debug {
		log = logger.NewConduitLogger(logrus.DebugLevel, "")
	}

	stdin, err := extractStdin(os.Stdin)
	if err != nil {
		log.Fatalf("failed to extract stdin: %v", err)
	}
	tlsCert, id, err := etcd.ParseCertFromBytes(stdin)
	if err != nil {
		log.Fatalf("failed to parse cert from stdin: %v", err)
	}

	var certPool *x509.CertPool
	caPath := viper.GetString(defaults.ConfigInternalCACertKey)
	if caPath != "" {
		certPool = x509.NewCertPool()
		caCert, err := loadCAFromFile(caPath)
		if err != nil {
			log.Fatalf("failed to get CA cert from [%v]: %v", caPath, err)
		}
		certPool.AddCert(caCert)
	}

	endpoints, err := util.GetEtcdEndpointsFromViper()
	if err != nil {
		log.Fatalf("failed to get etcd endpoints from viper: %v", endpoints)
	}

	em := etcd.NewETCDManager(log, tlsCert, certPool, endpoints)

	nodeList = os.Getenv("SLURM_JOB_NODELIST")

	it := proto.IncompleteTransfer(&proto.TransferDetails{TransferID: id.String()})

	return log, it, em, nodeList

}

// loadCAFromFile decodes the file at the specified path to return a CA certificate
func loadCAFromFile(CAPath string) (*x509.Certificate, error) {
	certBytes, err := os.ReadFile(CAPath)
	if err != nil {
		return nil, fmt.Errorf("error reading certificate from file: %v", err)
	}
	certBlock, _ := pem.Decode(certBytes)
	// cm.log.Infof("loaded %v: %v", certPath, certBlock.Type)
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("error parsing certificate from file: %v", err)
	}

	return cert, nil
}

// extractStdin will return all the bytes that are provided from stdin
func extractStdin(Stdin *os.File) ([]byte, error) {
	finalBytes := []byte{}

	r := bufio.NewReader(Stdin)
	buf := make([]byte, 0, 4*1024)
	for {
		n, err := r.Read(buf[:cap(buf)])
		buf = buf[:n]
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		finalBytes = append(finalBytes, buf...)
	}

	return finalBytes, nil
}

// StartPluginETCD sets the related keys in etcd to signal that the plugin has started on the FTA node
func StartPluginETCD(log *logger.ConduitLogger, c proto.SchedulerCommand, it proto.IncompleteTransfer, nodeList string, em *etcd.ETCDManager) (pErr proto.Error, err error, expiryQuit chan bool) {
	expiryQuit = updateTransferExpiry(it, em)

	// sometimes if conduit gets hammered with requests, it might not be able to tell etcd that the validation state was submitted before
	// we check for it here. Therefore we retry for a bit before giving up
	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	txnCompare := []clientv3.Cmp{}
	txnActions := []clientv3.Op{}

	etcdErrorKey := it.ETCDErrorKey()
	etcdStateKey := it.ETCDStateKey()

	submittedState, runningState, _, err := getCommandStates(c)
	if err != nil {
		return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to get command states for command[%s]: %v", c, err), expiryQuit
	}

	txnCompare = append(txnCompare, clientv3.Compare(clientv3.Value(etcdErrorKey), "=", proto.Error_ERROR_NONE.String()))
	txnCompare = append(txnCompare, clientv3.Compare(clientv3.Value(etcdStateKey), "=", submittedState.String()))

	txnActions = append(txnActions, clientv3.OpPut(etcdStateKey, runningState.String()))
	txnActions = append(txnActions, clientv3.OpPut(it.ETCDSchedulerNodesKey(c), nodeList))

	resp, err := em.RetryTxn(&txnCompare, &txnActions, retryCount, sleepDur)
	if err != nil {
		return proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("failed to set plugin start state in etcd: %v", err), expiryQuit
	}
	if !resp.Succeeded {
		state, _ := em.GetTransferState(it)
		errState, _ := em.RetryGet(it.ETCDErrorKey(), retryCount, sleepDur)
		errMessage, _ := em.RetryGet(it.ETCDErrorMessageKey(), retryCount, sleepDur)
		return proto.Error_ERROR_ETCD_INTERNAL, fmt.Errorf("setting plugin start state in etcd was unsuccessful:[state=%v,error=%v,errMessage=%v,responses=%+v]", state.String(), string(errState.Kvs[0].Value), string(errMessage.Kvs[0].Value), resp.Responses), expiryQuit
	}

	return proto.Error_ERROR_NONE, nil, expiryQuit
}

// CompletePluginETCD sets the related keys in etcd to signal that the plugin has ended on the FTA node
func CompletePluginETCD(log *logger.ConduitLogger, c proto.SchedulerCommand, it proto.IncompleteTransfer, em *etcd.ETCDManager, pluginData *plugin.PluginData, destInfo proto.DestInfo) (proto.Error, error) {
	// sometimes if conduit gets hammered with requests, it might not be able to tell etcd that the validation state was submitted before
	// we check for it here. Therefore we retry for a bit before giving up
	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	txnCompare := []clientv3.Cmp{}
	txnActions := []clientv3.Op{}

	etcdErrorKey := it.ETCDErrorKey()
	etcdStateKey := it.ETCDStateKey()

	_, runningState, completedState, err := getCommandStates(c)
	if err != nil {
		return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to get command states for command[%s]: %v", c, err)
	}

	// try to set the validation to complete
	txnCompare = []clientv3.Cmp{}
	txnActions = []clientv3.Op{}
	txnCompare = append(txnCompare, clientv3.Compare(clientv3.Value(etcdErrorKey), "=", proto.Error_ERROR_NONE.String()))
	txnCompare = append(txnCompare, clientv3.Compare(clientv3.Value(etcdStateKey), "=", runningState.String()))
	txnActions = append(txnActions, clientv3.OpPut(etcdStateKey, completedState.String()))

	if c == proto.SchedulerCommand_VALIDATION {
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

		allLeasesJSON, err := json.Marshal(&proto.Leases{
			Source:      sourceLeases,
			Destination: destinationLeases,
		})
		if err != nil {
			tErr := fmt.Errorf("failed to marshal lease list[%v, %v] into json for transfer: %v", sourceLeases, destinationLeases, err)
			return proto.Error_ERROR_CONDUIT_INTERNAL, tErr
			// log.Error(tErr)
			// transferError = tErr
		}

		txnActions = append(txnActions, clientv3.OpPut(it.ETCDLeasesKey(), string(allLeasesJSON)))
	}

	if destInfo != proto.DestInfo_DEST_NONE {
		// txnActions = append(txnActions, clientv3.OpPut(ft.transfer.ETCDFullDestinationsKey(), string(fullDestJSON)))
		txnActions = append(txnActions, clientv3.OpPut(it.ETCDDestInfoKey(), destInfo.String()))
	}

	if pluginData != nil {
		// json marshal plugin data, then compress with brotli
		jsonPluginData, err := json.Marshal(pluginData)
		if err != nil {
			log.Warnf("failed to marshal pluginData[%v] for etcd: %v", pluginData, err)
		}
		out := bytes.Buffer{}
		writer := brotli.NewWriterOptions(&out, brotli.WriterOptions{Quality: 1})
		in := bytes.NewReader(jsonPluginData)
		n, err := io.Copy(writer, in)
		if err != nil {
			return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to copy jsonPluginData to writer: %v", err)
		}

		if int(n) != len(jsonPluginData) {
			return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("copy did not copy the correct number of bytes: %v vs %v", int(n), len(jsonPluginData))
		}

		if err := writer.Close(); err != nil {
			return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to close brotli writer: %v", err)
		}
		txnActions = append(txnActions, clientv3.OpPut(it.ETCDPluginDataKey(), out.String()))
	}

	resp, err := em.RetryTxn(&txnCompare, &txnActions, retryCount, sleepDur)
	if err != nil {
		return proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("failed to set plugin complete state in etcd: %v", err)
	}
	if !resp.Succeeded {
		return proto.Error_ERROR_ETCD_INTERNAL, fmt.Errorf("setting plugin complete state in etcd was unsuccessful")
	}

	return proto.Error_ERROR_NONE, nil
}

// ErrorPluginETCD sets the related keys in etcd to signal that the plugin has encountered an error on the FTA node
func ErrorPluginETCD(log *logger.ConduitLogger, c proto.SchedulerCommand, it proto.IncompleteTransfer, em *etcd.ETCDManager, pluginErrors plugin.PluginErrors, pluginData *plugin.PluginData, destInfo proto.DestInfo) (proto.Error, error) {
	defer func(warnings []*plugin.FTAPathError, ict proto.IncompleteTransfer) {
		if len(warnings) > 0 {
			err := warnTransfer(log, em, ict, warnings, c)
			if err != nil {
				log.Errorf("failed to add warnings: %v", err)
			}
		}
	}(pluginErrors.Warnings, it)

	retryCount := viper.GetInt(defaults.ConfigFTAVerifyRetryCountKey)
	sleepDur := viper.GetDuration(defaults.ConfigFTAVerifySleepDurationKey)

	txnCompare := []clientv3.Cmp{}
	txnActions := []clientv3.Op{}

	etcdErrorKey := it.ETCDErrorKey()
	etcdErrorMessageKey := it.ETCDErrorMessageKey()

	pErr := proto.Error_ERROR_CONDUIT_INTERNAL
	var err error
	if len(pluginErrors.Errors) > 1 {
		// if we have more than one error, use the error status of the first one and cat all the other errors into the message
		for _, e := range pluginErrors.Errors {
			if err == nil {
				err = e.ErrMessage
			} else {
				err = fmt.Errorf("%v; %v", err, e.ErrMessage)
			}
			if pErr == proto.Error_ERROR_NONE && e.PErr != proto.Error_ERROR_NONE {
				pErr = e.PErr
			}
		}
	} else if len(pluginErrors.Errors) == 1 {
		for _, e := range pluginErrors.Errors {
			err = e.ErrMessage
			pErr = e.PErr
		}
	} else {
		return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("errorLease was called with no provided errors")
	}

	if c == proto.SchedulerCommand_VALIDATION {
		pErr = proto.Error_ERROR_VALIDATION
	}

	txnCompare = append(txnCompare, clientv3.Compare(clientv3.Value(etcdErrorKey), "=", proto.Error_ERROR_NONE.String()))
	// txnActions = append(txnActions, clientv3.OpPut(s.transfer.ETCDStateKey(), proto.LeaseState_LEASE_ERROR.String()))
	txnActions = append(txnActions, clientv3.OpPut(etcdErrorKey, pErr.String()))
	txnActions = append(txnActions, clientv3.OpPut(etcdErrorMessageKey, err.Error()))

	if destInfo != proto.DestInfo_DEST_NONE {
		// txnActions = append(txnActions, clientv3.OpPut(ft.transfer.ETCDFullDestinationsKey(), string(fullDestJSON)))
		txnActions = append(txnActions, clientv3.OpPut(it.ETCDDestInfoKey(), destInfo.String()))
	}

	if pluginData != nil {
		// json marshal plugin data, then compress with brotli
		jsonPluginData, err := json.Marshal(pluginData)
		if err != nil {
			log.Warnf("failed to marshal pluginData[%v] for etcd: %v", pluginData, err)
		}
		out := bytes.Buffer{}
		writer := brotli.NewWriterOptions(&out, brotli.WriterOptions{Quality: 1})
		in := bytes.NewReader(jsonPluginData)
		n, err := io.Copy(writer, in)
		if err != nil {
			return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to copy jsonPluginData to writer: %v", err)
		}

		if int(n) != len(jsonPluginData) {
			return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("copy did not copy the correct number of bytes: %v vs %v", int(n), len(jsonPluginData))
		}

		if err := writer.Close(); err != nil {
			return proto.Error_ERROR_CONDUIT_INTERNAL, fmt.Errorf("failed to close brotli writer: %v", err)
		}
		txnActions = append(txnActions, clientv3.OpPut(it.ETCDPluginDataKey(), out.String()))
	}

	resp, err := em.RetryTxn(&txnCompare, &txnActions, retryCount, sleepDur)
	if err != nil {
		return proto.Error_ERROR_ETCD_CONNECTION, fmt.Errorf("failed to set plugin error state in etcd: %v", err)
	}
	if !resp.Succeeded {
		return proto.Error_ERROR_ETCD_INTERNAL, fmt.Errorf("setting plugin complete error in etcd was unsuccessful")
	}

	return proto.Error_ERROR_NONE, nil
}

// warnTransfer simply adds warnings to etcd for a specific transfer
func warnTransfer(log *logger.ConduitLogger, em *etcd.ETCDManager, it proto.IncompleteTransfer, warnings []*plugin.FTAPathError, command proto.SchedulerCommand) error {
	warnlist := []string{}
	for _, w := range warnings {
		warnlist = append(warnlist, fmt.Sprintf("%s: %s", command, w.ErrMessage.Error()))
	}
	err := em.AddWarnings(it, warnlist)
	if err != nil {
		return fmt.Errorf("failed to add warnings in etcd: %v", err)
	}

	log.Debugf("successfully added warnings: %v", warnlist)
	return nil
}
