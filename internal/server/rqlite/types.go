// Copyright 2026. Triad National Security, LLC. All rights reserved.

package rqlite

type RqliteResponse struct {
	Results []*RqliteResult `json:"Results"`
}

type RqliteResult struct {
	Error        string     `json:"error"`
	Columns      []string   `json:"columns"`
	Values       [][]string `json:"values"`
	RowsAffected int64      `json:"rows_affected"`
}
