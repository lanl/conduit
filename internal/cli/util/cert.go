// Copyright 2026. Triad National Security, LLC. All rights reserved.

package util

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lanl/conduit/defaults"
	"github.com/spf13/viper"
)

var (
	TEST_MODE                 = false
	TEST_UserCertBundleExists = false
)

// GetCertAndKey will determine where the cert and key are with the provided the cert, key, and keypair flags
func GetUserCertAndKey(userCert string, userKey string, userCertBundle string, defaultCertBundlePath string) (cert string, key string, err error) {
	switch {
	case userCertBundle != defaultCertBundlePath:
		// always use provided keypair if it is different from the default
		return userCertBundle, userCertBundle, nil
	case userCert != "" && userKey == "":
		return userCert, userCert, nil
	case userCert == "" && userKey != "":
		return userKey, userKey, nil
	case userCert != "" && userKey != "":
		return userCert, userKey, nil
	case userCert == "" && userKey == "":
		var certBundlePath string
		var exists bool
		// only used for unit tests
		if TEST_MODE {
			certBundlePath = userCertBundle
			exists = TEST_UserCertBundleExists
		} else {
			// check to see if user keypair exists at default location
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return "", "", fmt.Errorf("failed to get user's home directory: %v", err)
			}

			certBundlePath = filepath.Join(homeDir, defaults.DefaultBundleName)

			exists, err = DoesKeyPairExist(certBundlePath)
			if err != nil {
				return "", "", err
			}
		}

		if exists {
			return certBundlePath, certBundlePath, nil
		} else {
			return "", "", nil
		}
	default:
		return "", "", fmt.Errorf("failed to determine which cert and key to use: [%v] [%v] [%v] [%v]", userCert, userKey, userCertBundle, defaultCertBundlePath)
	}

}

func DoesKeyPairExist(keypairPath string) (bool, error) {
	// check if overwritting existing cert
	if _, err := os.Stat(keypairPath); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, fmt.Errorf("failed to check for existing keypair at [%s]: %v", keypairPath, err)
	}
}

// GetCertPoolFromViper will first pull the cert pool from the system and then add the provided cert in the config if it exists
func GetCertPoolFromViper(viperKey string) (*x509.CertPool, error) {
	// Start with system cert pool
	certPool, err := x509.SystemCertPool()
	if err != nil {
		// If we can't get system certs, fall back to empty pool
		certPool = x509.NewCertPool()
	}

	// Add custom CA if provided
	caPath := viper.GetString(viperKey)
	if caPath != "" {
		caCert, err := loadCAFromFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load CA cert from [%v]: %v", caPath, err)
		}
		certPool.AddCert(caCert)
	}

	return certPool, nil
}

func loadCAFromFile(CAPath string) (*x509.Certificate, error) {
	certBytes, err := os.ReadFile(CAPath)
	if err != nil {
		return nil, fmt.Errorf("error reading certificate from file: %v", err)
	}
	certBlock, _ := pem.Decode(certBytes)
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("error parsing certificate from file: %v", err)
	}

	return cert, nil
}
