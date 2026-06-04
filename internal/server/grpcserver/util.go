// Copyright 2026. Triad National Security, LLC. All rights reserved.

package grpcserver

import (
	"context"
	"fmt"
	"os/user"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/jcmturner/goidentity/v6"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/spf13/viper"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type privLevel int

const (
	unprivileged privLevel = iota
	privilegedService
	privilegedAdmin
)

// filterTransfers will return a map of transfers that are filtered according to the provided QueryOptions
func filterTransfers(qo *proto.QueryOptions, transfers map[string]*proto.TransferDetails) (map[string]*proto.TransferDetails, error) {
	var finalTransfers map[string]*proto.TransferDetails

	// our operation changes whether we start with them all and remove or start from nothing and add on
	if qo.GetQueryOperation() == proto.QueryOperation_AND || len(qo.GetQueryMap()) == 0 {
		finalTransfers = transfers
	} else {
		finalTransfers = map[string]*proto.TransferDetails{}
	}

	// special case check:
	// check if we are only given a regex for transfer IDs. We can quickly filter through the transfer IDs to speed up this special case
	if len(qo.GetQueryMap()) == 1 {
		if r, ok := qo.GetQueryMap()["TransferID"]; ok {
			finalTransfers = map[string]*proto.TransferDetails{}
			reg, err := regexp.Compile(r)
			if err != nil {
				return make(map[string]*proto.TransferDetails), fmt.Errorf("failed to compile regex for key[TransferID]: %v", err)
			}
			for id, t := range transfers {
				if reg.MatchString(id) {
					finalTransfers[id] = t
				}
			}
		}
	}

	// go through the query map and check if each transfer matches
	for k, r := range qo.GetQueryMap() {
		reg, err := regexp.Compile(r)
		if err != nil {
			return make(map[string]*proto.TransferDetails), fmt.Errorf("failed to compile regex for key[%v]: %v", k, err)
		}
		for _, t := range transfers {
			vs := getValue(t, k)
			found := false
			for _, v := range vs {
				s, err := valueToString(v)
				if err != nil {
					return nil, fmt.Errorf("valueToString failed: %v", err)
				}
				if reg.MatchString(s) {
					found = true
					break
				}
			}
			switch qo.GetQueryOperation() {
			case proto.QueryOperation_AND:
				// in the AND operation, we remove the transfer if it didn't match this specific query regex
				if !found {
					delete(finalTransfers, t.GetTransferID())
				}
			case proto.QueryOperation_OR:
				// in the OR operation, we add the transfer if it did match this specific query regex
				if found {
					finalTransfers[t.GetTransferID()] = t
				}
			}
		}
	}

	return finalTransfers, nil
}

// valueToString converts a reflect.Value to a string depending on what underlying type it is
func valueToString(v reflect.Value) (string, error) {
	if !v.IsValid() {
		return "", fmt.Errorf("value is invalid: %v", v)
	}

	switch v.Type() {
	case reflect.TypeOf(""):
		// check if its already a string
		return v.String(), nil
	case reflect.TypeOf([]string{}):
		// check if its a slice of strings
		return strings.Join(v.Interface().([]string), " "), nil
	case reflect.TypeOf(timestamppb.Now()):
		// check if it's a timestamp and format it
		return v.Interface().(*timestamppb.Timestamp).AsTime().Local().Format(time.RFC3339), nil
	case reflect.TypeOf(uint32(0)):
		// check if it's a uint32
		return strconv.Itoa(int(v.Interface().(uint32))), nil
	default:
		// check if the value has a string function
		m := v.MethodByName("String")
		if m.IsValid() {
			rv := m.Call([]reflect.Value{})
			return rv[0].String(), nil
		} else {
			return "", fmt.Errorf("failed to convert reflect value to string: [%v] %v", v.Type(), v)
		}
	}
}

// getValue will return the value of the provided key
//
// if the provided key is in a nested map, it will return all the values for the field in all of the items in the map
func getValue(transfer *proto.TransferDetails, key string) []reflect.Value {
	ks := strings.Split(key, ".")
	tv := reflect.ValueOf(transfer).Elem()

	return recValue(tv, ks)
}

// recValue is used by getValue to recursively traverse a reflect value
func recValue(field reflect.Value, ks []string) []reflect.Value {
	f := field.FieldByName(ks[0])
	if len(ks) == 1 {
		// the key is not in a nested string
		return []reflect.Value{f}
	} else {
		if f.Kind() == reflect.Map && len(f.MapKeys()) > 0 {
			returnVals := []reflect.Value{}
			for _, k := range f.MapKeys() {
				returnVals = append(returnVals, recValue(f.MapIndex(k).Elem(), ks[1:])...)
			}
			return returnVals
		}
		if f.Kind() == reflect.Ptr {
			return recValue(f.Elem(), ks[1:])
		}
		return recValue(f, ks[1:])
	}
}

// createFields will return the string equivalent to every field in a TransferDetails object
func createFields() []string {
	v := reflect.ValueOf(proto.TransferDetails{SchedulerNodes: &proto.SchedulerNodes{}})

	return getFields(v, "")
}

// getFields is used by createFields to recursively traverse a TransferDetails object to get the field names
func getFields(v reflect.Value, prefix string) []string {
	fields := []string{}

	for i := 0; i < v.NumField(); i++ {
		if IsCapitalized(v.Type().Field(i).Name) {
			f := v.Field(i)
			switch f.Kind() {
			case reflect.Map:
				// check if map's values are structs
				mapValType := f.Type().Elem()
				if mapValType.Kind() == reflect.Ptr && mapValType.Elem().Kind() == reflect.Struct {
					// create a new instance of this type (note this creates a pointer)
					mapVal := reflect.New(f.Type().Elem().Elem())
					if prefix != "" {
						fs := getFields(mapVal.Elem(), fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name))
						fields = append(fields, fs...)
					} else {
						fs := getFields(mapVal.Elem(), v.Type().Field(i).Name)
						fields = append(fields, fs...)
					}
					continue
				}
			case reflect.Ptr:
				if f.Elem().Kind() == reflect.Struct {
					// create a new instance of this type (note this creates a pointer)
					structVal := reflect.New(f.Type().Elem())
					if prefix != "" {
						fs := getFields(structVal.Elem(), fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name))
						fields = append(fields, fs...)
					} else {
						fs := getFields(structVal.Elem(), v.Type().Field(i).Name)
						fields = append(fields, fs...)
					}
					continue
				}
			}
			if prefix != "" {
				fields = append(fields, fmt.Sprintf("%s.%s", prefix, v.Type().Field(i).Name))
			} else {
				fields = append(fields, v.Type().Field(i).Name)
			}
		}
	}

	return fields
}

// verifyKeys will verify when a provided key is in a list of fields
func verifyKeys(keys map[string]string, fields []string) (map[string]string, error) {
	qm := make(map[string]string)
	for k := range keys {
		if k == "" {
			continue
		}
		found := false
		for _, f := range fields {
			if strings.EqualFold(k, f) {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("failed to find key: [%v]", k)
		}
		qm[k] = keys[k]
	}
	return qm, nil
}

func IsCapitalized(s string) bool {
	rs := []rune(s)
	if !unicode.IsUpper(rs[0]) && unicode.IsLetter(rs[0]) {
		return false
	}
	return true
}

// isPrivileged will check if the provided user is in the predetermined lists of privileged admins and services
func isPrivileged(user string) privLevel {
	for _, u := range privilegedServices {
		if user == u {
			return privilegedService
		}
	}
	for _, u := range privilegedAdmins {
		if user == u {
			return privilegedAdmin
		}
	}

	return unprivileged
}

// getUserFromRequest retrieves the kerberos user from the gRPC request
// if this is an admin doing actions on behalf of a user, then privilegedAccess will be true
func (s *ConduitServer) getUserFromRequest(ctx context.Context, requestedUser *string) (moniker string, reqPrivLevel privLevel, err error) {
	// 1. check if grpc is authenticated
	id, ok := ctx.Value(goidentity.CTXKey).(goidentity.Identity)
	if ok {
		user := id.UserName()
		if requestedUser != nil && *requestedUser == user {
			return user, unprivileged, nil
		}

		if viper.GetString(defaults.ConfigLDAPHostKey) != "" {
			// get ldap config
			l, err := NewLDAP(
				viper.GetString(defaults.ConfigLDAPHostKey),
				viper.GetInt(defaults.ConfigLDAPPortKey),
				viper.GetStringSlice(defaults.ConfigLDAPBaseDNKey),
				viper.GetStringSlice(defaults.ConfigLDAPKrb5AttributesKey),
				viper.GetStringSlice(defaults.ConfigLDAPUnameAttributesKey),
				viper.GetStringSlice(defaults.ConfigLDAPUIDNumber5AttributesKey),
			)
			if err != nil {
				return "", unprivileged, err
			}

			lUser, err := l.ldapSearch(id, 0, defaults.DefaultLDAPTimeout)
			if err != nil {
				return "", unprivileged, fmt.Errorf("failed to search ldap for user[%v]: %v", user, err)
			}

			s.log.Debugf("found uid[%v] in ldap for kerberos user[%v@%v]", lUser, id.UserName(), id.Domain())
			return lUser, unprivileged, nil
		} else {
			// use the user in the kerberos principle if ldap isn't configured
			return user, unprivileged, nil
		}
	}
	s.log.Debugf("identity not provided through kerberos")

	// 2. check if tls client cert was provided
	p, ok := peer.FromContext(ctx)
	if !ok {
		err = fmt.Errorf("failed to get peer from context")
		return "", unprivileged, err
	}
	tlsAuth, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		err = fmt.Errorf("unexpected peer transport credentials")
		return "", unprivileged, err
	}

	commonName := ""

	if len(tlsAuth.State.VerifiedChains) > 0 && len(tlsAuth.State.VerifiedChains[0]) > 0 {
		// Check subject common name against configured username
		commonName = tlsAuth.State.VerifiedChains[0][0].Subject.CommonName
	} else {
		err = fmt.Errorf("neither TLS client certs nor kerberos creds were provided in the request")
		return "", unprivileged, err
	}

	// 3. if tls client cert was provided, is it a privileged service acting on behalf of someone?
	reqPrivLevel = isPrivileged(commonName)
	if reqPrivLevel == privilegedService || reqPrivLevel == privilegedAdmin {
		s.log.Debugf("found privileged user[%v] with level[%v]. Requested user: %s", commonName, reqPrivLevel, requestedUser)
		if requestedUser == nil || *requestedUser == "" {
			// return "", reqPrivLevel, fmt.Errorf("a user must be provided when making privileged requests")
			return "", reqPrivLevel, nil
		} else if *requestedUser == "root" {
			return "", reqPrivLevel, fmt.Errorf("user root is not allowed to use conduit")
		} else {
			// check if we were given a uid number instead of a username
			uidNumber, err := strconv.ParseInt(*requestedUser, 10, 64)
			if err != nil {
				s.log.Debugf("client cert came from %v. Acting on behalf on provided user: %v", commonName, *requestedUser)
				return *requestedUser, reqPrivLevel, nil
			} else {
				// check if ldap is configured
				if viper.GetString(defaults.ConfigLDAPHostKey) != "" {
					// use ldap to get uid for provided uid number
					l, err := NewLDAP(
						viper.GetString(defaults.ConfigLDAPHostKey),
						viper.GetInt(defaults.ConfigLDAPPortKey),
						viper.GetStringSlice(defaults.ConfigLDAPBaseDNKey),
						viper.GetStringSlice(defaults.ConfigLDAPKrb5AttributesKey),
						viper.GetStringSlice(defaults.ConfigLDAPUnameAttributesKey),
						viper.GetStringSlice(defaults.ConfigLDAPUIDNumber5AttributesKey),
					)
					if err != nil {
						return "", unprivileged, err
					}

					lUser, err := l.ldapSearch(nil, uidNumber, defaults.DefaultLDAPTimeout)
					if err != nil {
						return "", unprivileged, fmt.Errorf("failed to search ldap for user[%v]: %v", uidNumber, err)
					}
					s.log.Debugf("found uid[%v] in ldap for provided uid number[%v]", lUser, uidNumber)

					return lUser, reqPrivLevel, nil
				} else {
					// ldap was not configured. Try looking up user directly on the machine
					u, err := user.LookupId(*requestedUser)
					if err != nil {
						return "", reqPrivLevel, fmt.Errorf("%v provided a uid %v in request, but couldn't find user: %v", commonName, *requestedUser, err)
					} else {
						if u.Uid == "0" {
							return "", reqPrivLevel, fmt.Errorf("user root is not allowed to use conduit")
						}
						s.log.Debugf("%v provided a uid %v in request. Using username: %v", commonName, *requestedUser, u.Username)
						return u.Username, reqPrivLevel, nil
					}
				}
			}
		}
	} else if commonName == "root" {
		err := fmt.Errorf("user root is not allowed to use conduit")
		return "", reqPrivLevel, err
	} else if commonName != "" {
		s.log.Debugf("found client cert user [%v] in request", commonName)
		return commonName, reqPrivLevel, nil
	}

	err = fmt.Errorf("client cert CN was empty")
	s.log.Error(err)
	return "", reqPrivLevel, err
}
