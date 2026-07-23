// Copyright 2026. Triad National Security, LLC. All rights reserved.

package rqlite

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/doug-martin/goqu/v9"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/logger"
	"github.com/lanl/conduit/internal/server/rqlite/util"
	"google.golang.org/protobuf/encoding/protojson"
	_ "google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	ConduitTable     = "conduit"
	ColumnTransferId = "transfer_id"
	ColumnUser       = "user"
	ColumnTimestamp  = "created_timestamp"
	ColumnTransfer   = "transfer"
)

type RqliteManager struct {
	log *logger.ConduitLogger

	client          *http.Client
	rqliteEndpoints []string
}

func NewRqliteManager(log *logger.ConduitLogger, tlsCert *tls.Certificate, certPool *x509.CertPool) (*RqliteManager, error) {
	l := logger.NewConduitLogger(log.GetLevel(), fmt.Sprintf("%srqlite manager:", log.GetPrefix()))
	if log.GetPrefix() == "" {
		l = logger.NewConduitLogger(log.GetLevel(), "rqlite manager:")
	}

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{*tlsCert},
		RootCAs:      certPool,
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{
		Transport: transport,
		Timeout:   defaults.DefaultRqliteTimeout,
	}

	// get all addresses for all rqlite instances
	rEndpoints, err := util.GetRqliteEndpointsFromViper()
	if err != nil {
		return nil, err
	}

	rm := &RqliteManager{
		log:             l,
		client:          client,
		rqliteEndpoints: rEndpoints,
	}

	return rm, nil
}

// adds a transfer to rqlite
func (rm *RqliteManager) AddTransfer(td *proto.TransferDetails) error {
	// convert transfer details to json encoded string
	jsonBytes, err := protojson.Marshal(td)
	if err != nil {
		return fmt.Errorf("failed to marshal transfer details: %v", err)
	}

	createdTime := td.GetCreatedTime().AsTime().UTC()
	createdTimeString := createdTime.Format(time.DateTime)

	// generate sql string
	ds := goqu.Insert(ConduitTable).Cols(ColumnTransferId, ColumnUser, ColumnTimestamp, ColumnTransfer).Vals(
		goqu.Vals{td.GetTransferID(), td.GetUser(), createdTimeString, string(jsonBytes)},
	)
	sql, _, err := ds.ToSQL()
	if err != nil {
		return fmt.Errorf("failed to generate sql command for transfer: %v", err)
	}

	// send sql to rqlite
	_, err = rm.sendRequest(util.PathExecute, sql)
	if err != nil {
		return fmt.Errorf("failed to add transfer to rqlite: %v", err)
	}

	return nil
}

// gets all transfers in rqlite for user. If user is nil, it returns all transfers in rqlite
func (rm *RqliteManager) GetTransfersByUser(user *string) (map[string]*proto.TransferDetails, error) {
	// generate sql string
	ds := goqu.From(ConduitTable)
	if user != nil {
		ds = ds.Where(goqu.Ex{
			ColumnUser: []string{*user},
		})
	}
	sql, _, err := ds.ToSQL()
	if err != nil {
		return nil, fmt.Errorf("failed to generate sql command for transfer: %v", err)
	}

	// send sql to rqlite
	qr, err := rm.sendRequest(util.PathQuery, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get all transfers from rqlite: %v", err)
	}

	// convert json encoded transfer details back into real transfer details objects
	transfers := make(map[string]*proto.TransferDetails)
	for _, result := range qr.Results {
		for _, row := range result.Values {
			t := &proto.TransferDetails{}
			err = protojson.Unmarshal([]byte(row[3]), t)
			if err != nil {
				// just log this error instead of erroring out. This will happen anytime the api changes, which is unfortunate...
				rm.log.Errorf("failed to unmarshal transfer from rqlite, did the api change?[%v]: %v", row[3], err)
				continue
			}

			transfers[t.GetTransferID()] = t
		}
	}

	return transfers, nil
}

// gets all transfers in rqlite for user. If user is nil, it returns all transfers in rqlite
func (rm *RqliteManager) GetTransfers(transferIDs []string) (map[string]*proto.TransferDetails, error) {
	// generate sql string
	ds := goqu.From(ConduitTable)
	ds = ds.Where(goqu.Ex{
		ColumnTransferId: transferIDs,
	})
	sql, _, err := ds.ToSQL()
	if err != nil {
		return nil, fmt.Errorf("failed to generate sql command for transfer: %v", err)
	}

	// send sql to rqlite
	qr, err := rm.sendRequest(util.PathQuery, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get all transfers from rqlite: %v", err)
	}

	// convert json encoded transfer details back into real transfer details objects
	transfers := make(map[string]*proto.TransferDetails)
	for _, result := range qr.Results {
		for _, row := range result.Values {
			t := &proto.TransferDetails{}
			err = protojson.Unmarshal([]byte(row[3]), t)
			if err != nil {
				// just log this error instead of erroring out. This will happen anytime the api changes, which is unfortunate...
				rm.log.Errorf("failed to unmarshal transfer from rqlite, did the api change?[%v]: %v", row[3], err)
				continue
			}

			transfers[t.GetTransferID()] = t
		}
	}

	return transfers, nil
}

// creates the conduit table in rqlite
func (rm *RqliteManager) CreateTable() error {
	// Manually create sql create table command
	sql := fmt.Sprintf("CREATE TABLE %s (%s TINYTEXT NOT NULL UNIQUE, %s TINYTEXT NOT NULL, %s TIMESTAMP NOT NULL, %s LONGTEXT NOT NULL)", ConduitTable, ColumnTransferId, ColumnUser, ColumnTimestamp, ColumnTransfer)

	// send sql to rqlite
	_, err := rm.sendRequest(util.PathExecute, sql)
	if err != nil {
		return fmt.Errorf("failed to create rqlite table: %v", err)
	}

	return nil
}

// generates a http request and sends it to rqlite
func (rm *RqliteManager) sendRequest(urlPath util.RqlitePath, sql string) (*RqliteResponse, error) {
	// convert sql command to a json encoded array
	sqlCommands := []string{sql}
	jsonSQLCommands, err := json.Marshal(sqlCommands)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal sql[%v] into json: %v", sqlCommands, err)
	}

	var fErr error

	// iterate over the rqlite endpoints in a random order
	perm := rand.Perm(len(rm.rqliteEndpoints))
	for _, i := range perm {
		url, err := url.Parse(fmt.Sprintf("%v%v%v", "https://", rm.rqliteEndpoints[i], urlPath))
		if err != nil {
			fErr = fmt.Errorf("failed to create url: %v", err)
			continue
		}

		// create http request to send to rqlite
		req, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(jsonSQLCommands))
		if err != nil {
			fErr = fmt.Errorf("failed to build request: %v", err)
			continue
		}

		resp, err := rm.client.Do(req)
		defer func() {
			if resp != nil {
				resp.Body.Close()
			}
		}()

		if err != nil || resp.StatusCode != http.StatusOK {
			rm.log.Errorf("rqlite responded with an error: [%+v] [%v]", resp, err)
			fErr = fmt.Errorf("error during rqlite request: %v", err)
			continue
		}

		qr := &RqliteResponse{}
		err = json.NewDecoder(resp.Body).Decode(&qr)
		if err != nil {
			fErr = fmt.Errorf("failed to unmarshal rqlite response: %v", err)
			continue
		}

		// check for errors in any results
		var errs error
		for _, r := range qr.Results {
			if r.Error != "" {
				if errs != nil {
					errs = fmt.Errorf("%v , %v", errs, r.Error)
				} else {
					errs = fmt.Errorf("%v", r.Error)
				}
			}
		}

		if errs != nil {
			if errs.Error() == fmt.Sprintf("table %s already exists", ConduitTable) {
				rm.log.Warn(errs)
			} else {
				fErr = fmt.Errorf("rqlite returned an error: %v", errs)
				continue
			}
		}

		return qr, nil
	}

	return nil, fmt.Errorf("failed to send rqlite to any endpoints: %v", fErr)
}
