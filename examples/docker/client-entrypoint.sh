#!/bin/bash

cp /etc/conduit/keys/conduit-external-ca.pem /etc/ssl/certs/conduit-external-ca.pem
update-ca-certificates --verbose --fresh

echo 'password' | sudo -u testuser kinit testuser

sudo -u testuser /bin/bash
