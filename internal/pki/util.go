// Copyright 2026. Triad National Security, LLC. All rights reserved.

package pki

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/lanl/conduit/defaults"
	"github.com/spf13/viper"
)

// generateCACert will generate a fresh CA certificate that will expire at notAfter
func generateCACert(notAfter time.Time) (*x509.Certificate, error) {
	serialNumber, err := generateSerialNumber()
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number: %v", err)
	}

	organization := viper.GetString(defaults.ConfigCertOrganizationKey)
	country := viper.GetString(defaults.ConfigCertCountryKey)
	province := viper.GetString(defaults.ConfigCertProvinceKey)
	locality := viper.GetString(defaults.ConfigCertLocalityKey)
	postalCode := viper.GetString(defaults.ConfigCertPostalCodeKey)

	ca := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
			Country:      []string{country},
			Province:     []string{province},
			Locality:     []string{locality},
			PostalCode:   []string{postalCode},
			CommonName:   "conduit-ca",
		},
		NotBefore:             time.Now(),
		NotAfter:              notAfter,
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageKeyEncipherment,
		BasicConstraintsValid: true,
	}

	return ca, nil
}

// generateServerCert will generate a fresh server certificate with the specified IPs and hostnames and will expire at notAfter
func generateServerCert(netIPs []net.IP, hostnames []string, commonName string, notAfter time.Time) (*x509.Certificate, error) {
	serialNumber, err := generateSerialNumber()
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number: %v", err)
	}

	organization := viper.GetString(defaults.ConfigCertOrganizationKey)
	country := viper.GetString(defaults.ConfigCertCountryKey)
	province := viper.GetString(defaults.ConfigCertProvinceKey)
	locality := viper.GetString(defaults.ConfigCertLocalityKey)
	postalCode := viper.GetString(defaults.ConfigCertPostalCodeKey)

	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
			Country:      []string{country},
			Province:     []string{province},
			Locality:     []string{locality},
			PostalCode:   []string{postalCode},
			CommonName:   commonName,
		},
		NotBefore:   time.Now(),
		NotAfter:    notAfter,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	if len(hostnames) > 0 {
		cert.DNSNames = hostnames
	}

	if len(netIPs) > 0 {
		cert.IPAddresses = netIPs
	}

	// if cn != "" {
	// 	cert.EmailAddresses = []string{cn}
	// }

	return cert, nil
}

// GenerateClientCert will generate a client certificate with the specified commonName and will expire at notAfter
func GenerateClientCert(commonName string, notAfter time.Time) (*x509.Certificate, error) {
	serialNumber, err := generateSerialNumber()
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number: %v", err)
	}

	organization := viper.GetString(defaults.ConfigCertOrganizationKey)
	country := viper.GetString(defaults.ConfigCertCountryKey)
	province := viper.GetString(defaults.ConfigCertProvinceKey)
	locality := viper.GetString(defaults.ConfigCertLocalityKey)
	postalCode := viper.GetString(defaults.ConfigCertPostalCodeKey)

	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
			Country:      []string{country},
			Province:     []string{province},
			Locality:     []string{locality},
			PostalCode:   []string{postalCode},
			CommonName:   commonName,
		},
		NotBefore:   time.Now(),
		NotAfter:    notAfter,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	return cert, nil
}

// signCert will return the bytes of a signed certificate
func signCert(cert, signingCA *x509.Certificate, signingCAKey crypto.PrivateKey, certPubKey crypto.PublicKey) ([]byte, error) {
	certBytes, err := x509.CreateCertificate(rand.Reader, cert, signingCA, certPubKey, signingCAKey)
	return certBytes, err
}

// certToPEM will convert the bytes of a certificate to a PEM format
func certToPEM(certBytes []byte) (*bytes.Buffer, error) {
	certPEM := new(bytes.Buffer)
	err := pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})
	return certPEM, err
}

// privKeyToPEM will convert a private key to the bytes of a PEM format
func privKeyToPEM(certPrivKey crypto.PrivateKey) (*bytes.Buffer, error) {
	certPrivKeyPEM := new(bytes.Buffer)
	privKeyBytes, err := x509.MarshalPKCS8PrivateKey(certPrivKey)
	if err != nil {
		return nil, err
	}
	err = pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privKeyBytes,
	})
	return certPrivKeyPEM, err
}

// loadCredsFromFile will return the cert and key that are located at the specified file paths
func loadCredsFromFile(certPath, keyPath string) (*x509.Certificate, crypto.PrivateKey, error) {
	certBytes, err := os.ReadFile(certPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("failed to read certificate from file: %v", err)
	}
	certBlock, _ := pem.Decode(certBytes)
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse certificate: %v", err)
	}

	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("failed to read key from file: %v", err)
	}
	keyBlock, _ := pem.Decode(keyBytes)
	key, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse key: %v", err)
	}
	return cert, key, nil
}

// writeCredsToFile will write a cert and key to file
//
// if certPath and keyPath are equal, this function will combine the cert and key into a single PEM file
func writeCredsToFile(signingCA *x509.Certificate, signingCAKey crypto.PrivateKey, certPath, keyPath string, cert *x509.Certificate, certKey crypto.PrivateKey) error {
	pubKey, err := PublicKeyFromPrivateKey(certKey)
	if err != nil {
		return fmt.Errorf("failed to get public key from private key: %v", err)
	}

	signedCertBytes, err := signCert(cert, signingCA, signingCAKey, pubKey)
	if err != nil {
		return fmt.Errorf("failed to sign cert: %v", err)
	}

	certPEM, err := certToPEM(signedCertBytes)
	if err != nil {
		return fmt.Errorf("failed convert cert to PEM: %v", err)
	}

	privKeyPEM, err := privKeyToPEM(certKey)
	if err != nil {
		return fmt.Errorf("error getting private key pem[%v]: %v", keyPath, err)
	}

	if certPath == keyPath {
		certAndKey := append(certPEM.Bytes(), privKeyPEM.Bytes()...)
		err = os.WriteFile(certPath, certAndKey, 0600)
		if err != nil {
			return fmt.Errorf("error writing cert pem to file %v: %v", certPath, err)
		}
	} else {
		err = os.WriteFile(certPath, certPEM.Bytes(), 0644)
		if err != nil {
			return fmt.Errorf("error writing cert pem to file %v: %v", certPath, err)
		}
		err = os.WriteFile(keyPath, privKeyPEM.Bytes(), 0600)
		if err != nil {
			return fmt.Errorf("error writing key pem to file %v : %v", keyPath, err)
		}
	}
	return nil
}

// generateSerialNumber generates a serial number to be used in an x509 cert
func generateSerialNumber() (*big.Int, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}
	return serialNumber, nil
}

// GetKeyPairFromFile takes in paths to a cert and key and returns a tls keypair. Used in conduit-cli
func GetKeyPairFromFile(certPath, keyPath string) (*tls.Certificate, error) {
	certBytes, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate from file[%s]: %v", certPath, err)
	}

	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read key from file[%s]: %v", certPath, err)
	}

	tlsCert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create tls key pair: %v", err)
	}

	return &tlsCert, nil
}

func PublicKeyFromPrivateKey(priv crypto.PrivateKey) (crypto.PublicKey, error) {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &k.PublicKey, nil

	case *ecdsa.PrivateKey:
		return &k.PublicKey, nil

	case ed25519.PrivateKey:
		return k.Public().(ed25519.PublicKey), nil

	default:
		return nil, errors.New("unsupported private key type")
	}
}
