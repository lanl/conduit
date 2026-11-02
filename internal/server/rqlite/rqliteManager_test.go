// Copyright 2026. Triad National Security, LLC. All rights reserved.

package rqlite

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lanl/conduit/internal/server/rqlite/util"
)

func TestRemoveTransfers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reply   string
		want    int64
		wantErr bool
	}{
		{"removed", `{"results":[{"rows_affected":1}]}`, 1, false},
		{"already removed", `{"results":[{"rows_affected":0}]}`, 0, false},
		{"SQL error", `{"results":[{"error":"delete failed"}]}`, 0, true},
		{"missing result", `{"results":[]}`, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != string(util.PathExecute) {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				var statements []string
				if err := json.NewDecoder(r.Body).Decode(&statements); err != nil {
					t.Errorf("failed to decode SQL: %v", err)
				}
				if len(statements) != 1 || !strings.Contains(statements[0], "DELETE FROM") ||
					!strings.Contains(statements[0], "transfer-1") {
					t.Errorf("unexpected SQL: %v", statements)
				}
				_, _ = w.Write([]byte(tc.reply))
			}))
			defer server.Close()

			rm := &RqliteManager{
				client:          server.Client(),
				rqliteEndpoints: []string{strings.TrimPrefix(server.URL, "https://")},
			}
			removed, err := rm.RemoveTransfers([]string{"transfer-1"})
			if (err != nil) != tc.wantErr || removed != tc.want {
				t.Fatalf("RemoveTransfers() = (%d, %v), want (%d, error=%t)", removed, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestRemoveTransfersEmpty(t *testing.T) {
	removed, err := (&RqliteManager{}).RemoveTransfers(nil)
	if err != nil || removed != 0 {
		t.Fatalf("RemoveTransfers(nil) = (%d, %v), want (0, nil)", removed, err)
	}
}
