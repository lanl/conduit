// Copyright 2026. Triad National Security, LLC. All rights reserved.

package querygen

import (
	"fmt"
	"regexp"

	"github.com/lanl/conduit/internal/cli/processing"
)

// StrType type for Conduit datatypes
type StrType int

// StrType names
const (
	TypeUnknown StrType = iota
	TypeTimestamp
	TypeTransferID
	TypeSlurmJobIDComment
	TypeSlurmJobIDCommentGeneric
	TypePath
	TypeComment
)

// A map of attributes that are searchable in transfer
var queryAttributes = map[string]string{
	"createdTime": "CreatedTime",
	"destination": "Destination",
	"endTime":     "EndTime",
	"expiry":      "Expiry",
	"source":      "Source",
	"startTime":   "StartTime",
	"state":       "State",
	"transferID":  "TransferID",
	"user":        "User",
	"comment":     "Comment",
}

var customStrTypeRegex = map[StrType]string{
	TypeSlurmJobIDCommentGeneric: `^SLURMJOB:%v,`,
	TypeSlurmJobIDComment:        `^%v`,
	TypeComment:                  `^%v$`,
}

// Map association between StrType and printable string
var strTypeNames = map[StrType]string{
	TypeUnknown:    "Unknown",
	TypeTimestamp:  "Timestamp",
	TypeTransferID: "TransferID",
	TypePath:       "Path",
}

// Get capitalized string without error handling
func toCapitalize(in string) string {
	out, _ := processing.ProcessCaps(in)
	return out
}

// Stringer implementation for StrType
func (st StrType) String() string {
	return strTypeNames[st]
}

// Regex string list for Timestamp
var regexTimestamp = []string{
	// 2022-09-19T18:34:13-06:00
	"([0-9]){4}-([0-9]){2}-([0-9]){2}T([0-9]){2}:([0-9]){2}:([0-9]){2}-([0-9]){2}:([0-9]){2}",
	// 2022-09-19
	"([0-9]){4}-([0-9]){2}-([0-9]){2}",
	// 2022-09
	"([0-9]){4}-([0-9]){2}",
	// 09-19
	"([0-9]){2}-([0-9]){2}",
	// T18:34:13-06:00
	"T([0-9]){2}:([0-9]){2}:([0-9]){2}-([0-9]){2}:([0-9]){2}",
	// T18:34:13
	"T([0-9]){2}:([0-9]){2}:([0-9]){2}",
	// T18:34
	"T([0-9]){2}:([0-9]){2}",
	// 18:34:13
	"([0-9]){2}:([0-9]){2}:([0-9]){2}",
	// 18:34
	"([0-9]){2}:([0-9]){2}",
	// Feb 17 15:39
	// Would be useful, but can't convert to exact timestamp to match against CONDUIT-stored timestamps.
	//"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+(([1-9][0-9])|([1-9]))\\s+(((1[0-9])|(2[0-3])|(0[0-9]):([0-5][0-9]))|([0-9]{4}))",
}

// Regex string list for TransferID
var regexTransferID = []string{
	// 19783d8e-1a21-4f1a-b52b-68cbf34119dc
	"([a-f0-9]){8}-([a-f0-9]){4}-([a-f0-9]){4}-([a-f0-9]){4}-([a-f0-9]){12}",
	// 19783d8e-1a21-4f1a-b52b
	"([a-f0-9]){8}-([a-f0-9]){4}-([a-f0-9]){4}-([a-f0-9]){4}",
	// 19783d8e-1a21-4f1a
	"([a-f0-9]){8}-([a-f0-9]){4}-([a-f0-9]){4}",
	// 19783d8e-1a21
	"([a-f0-9]){8}-([a-f0-9]){4}",
	// 19783d8e
	"([a-f0-9]){8}",
	// 1a21
	"([a-f0-9]){4}",
}

// Regex string list for SlurmJobID
var regexSlurmJobIDGeneric = []string{
	"^[0-9]+$",
}

// Regex string list for SlurmJobID
var regexSlurmJobID = []string{
	"^SLURMJOB:[0-9]+,$",
}

// Regex string list for path (POSIX)
var regexPath = []string{
	"(/)?[a-zA-Z0-9_+/.-]+",
}

// Regex string list for Comment
var regexComment = []string{
	"^[0-9]+$",
	"^SLURMJOB:[0-9]+,SLURMINDEX:[0-9]+,SLURMTYPE:(?:CONDUIT_PRE|CONDUIT_POST)$",
}

// Map of type to slice of regex strings
var regexStrings = map[StrType][]string{
	TypeTransferID:               regexTransferID,
	TypeTimestamp:                regexTimestamp,
	TypePath:                     regexPath,
	TypeSlurmJobIDCommentGeneric: regexSlurmJobIDGeneric,
	TypeSlurmJobIDComment:        regexSlurmJobID,
	TypeComment:                  regexComment,
}

// String list of transfer attributes associated with Timestamp
var queryTimestamp = []string{
	queryAttributes["createdTime"],
	queryAttributes["endTime"],
	//queryAttributes["leases_expiry"],
	queryAttributes["startTime"],
}

// String list of transfer attributes associated with TransferID
var queryTransferID = []string{
	queryAttributes["transferID"],
}

// String list of transfer attributes associated with Path
var queryPath = []string{
	queryAttributes["destination"],
	queryAttributes["source"],
}

// String list of transfer attributes associated with comments
var queryComment = []string{
	queryAttributes["comment"],
}

// Map of type to slice of query strings representing attributes to search
// in transfer
var queryStrings = map[StrType][]string{
	TypeTimestamp:                queryTimestamp,
	TypeTransferID:               queryTransferID,
	TypePath:                     queryPath,
	TypeSlurmJobIDCommentGeneric: queryComment,
	TypeSlurmJobIDComment:        queryComment,
	TypeComment:                  queryComment,
}

// Function to return detected type from string
func getTypes(source string) []StrType {
	rtnTypes := []StrType{}
	for regex_type, regex_set := range regexStrings {
		for _, regex_str := range regex_set {
			if regexp.MustCompile(regex_str).MatchString(source) {
				rtnTypes = append(rtnTypes, regex_type)
				break
			}
		}
	}
	return rtnTypes
}

func GenerateQueryStringMap(input []string) map[string]string {
	queryMap := make(map[string]string)
	// For each input string,
	for _, instr := range input {
		// Try to determine type from input
		stypes := getTypes(instr)
		// If determined type is known,
		if len(stypes) > 0 {
			for _, stype := range stypes {
				// For each query string associated with this type,
				for _, qstring := range queryStrings[stype] {
					// check if queryMap already has a value for this query string
					if val, ok := queryMap[qstring]; ok {
						// Add query string / input string association to regex
						queryMap[qstring] = fmt.Sprintf("%s|%s", val, getCustomStrTypeRegex(stype, instr))
					} else {
						// Add query string / input string association
						queryMap[qstring] = getCustomStrTypeRegex(stype, instr)
					}
				}
			}
		} else {
			// If determined type is unknown,
			// For each possible query attribute,
			for _, qstring := range queryAttributes {
				// check if queryMap already has a value for this query string
				if val, ok := queryMap[qstring]; ok {
					// Add query string / input string association to regex
					queryMap[qstring] = fmt.Sprintf("%s|%s", val, instr)
				} else {
					// Add query string / input string association
					queryMap[qstring] = instr
				}
			}
		}
	}
	return queryMap
}

func getCustomStrTypeRegex(sType StrType, instr string) string {
	if fs, ok := customStrTypeRegex[sType]; ok {
		return fmt.Sprintf(fs, instr)
	} else {
		return instr
	}
}

func main() {
	tests := []string{
		"2022-09-19T18:34:13-06:00",
		"19783d8e-1a21-4f1a-b52b-68cbf34119dc",
		"/mnt/fs_2/bar/",
	}
	query := GenerateQueryStringMap(tests)
	fmt.Println(query)
	fmt.Println("")
}
