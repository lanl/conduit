# Errant Paths

Errant paths are simply paths that have some sort of error that the user needs to resolve. Currently this is only used for prompting a user to check a transfer trash if that transfers source was altered during transfer. Errant paths are never added directly by conduit, they are only read out of etcd.

## Adding Errant Paths

To add an errant path to etcd you will need an etcd root and cert key pair that's been signed by the conduit internal CA. The errored path needs to be encoded with [html url encoding standards](https://www.w3schools.com/tags/ref_urlencode.ASP).

The etcd key for an errant path is this format:

```
errors/<username>/<url encoded path>
```

The value is an RFC3339 formatted timestamp. This timestamp is used to lock out a user from using conduit if this errant path isn't resolved by the configured amount of time (see `errant-lock` in `conduit-server-config.yaml`).

### Example using etcdctl:

```sh
# adding errant path for testuser's errored data at /trashpath/testuser/erroreddata

./etcdctl \
--cert /conduit/admin/keys/etcd-client-cert.pem \
--key /conduit/admin/keys/etcd-client-key.pem \
--cacert /conduit/conduit-server/keys/conduit-internal-ca.pem \
--endpoints=<ip-address-of-etcd-server>:2379 \
put errors/testuser/%2Ftrashpath%2Ftestuser%2Ferroreddata 2023-09-24T15:30:00+09:00
```

The user will see this message when running any conduit commands:

```
 __        __     _      ____    _   _   ___   _   _    ____
 \ \      / /    / \    |  _ \  | \ | | |_ _| | \ | |  / ___|
  \ \ /\ / /    / _ \   | |_) | |  \| |  | |  |  \| | | |  _
   \ V  V /    / ___ \  |  _ <  | |\  |  | |  | |\  | | |_| |
    \_/\_/    /_/   \_\ |_| \_\ |_| \_| |___| |_| \_|  \____|
You have failed transfer trash that needs to be cleaned up:
/trashpath/testuser/erroreddata
Please manually move or delete trash and then use the "conduit purge" command to continue using conduit
```

## Removing Errant Paths

### Users

Users can remove errant paths with the conduit-cli `purge` command. Here's an example command that a user would use after an errant path is resolved:

```sh
conduit purge /trashpath/testuser/erroreddata
```

when purged from etcd the key will remain in etcd, but the timestamp will be set to `0001-01-01T00:00:00Z` ([which is a the value of a new empty timestamp in timestamppb](https://pkg.go.dev/google.golang.org/protobuf/types/known/timestamppb#New))

### Admin

Admins can use conduit-cli with the `--user` flag to purge a users errant paths. Although this will **not** remove it from etcd.

Etcdctl or another etcd client can be used as an alternative to conduit-cli and is able to delete the key from the errors section of conduit. Example of that here:

```sh
# removing an errant path for testuser's errored data at /trashpath/testuser/erroreddata

./etcdctl \
--cert /conduit/admin/keys/etcd-client-cert.pem \
--key /conduit/admin/keys/etcd-client-key.pem \
--cacert /conduit/conduit-server/keys/conduit-internal-ca.pem \
--endpoints=<ip-address-of-etcd-server>:2379 \
del errors/testuser/%2Ftrashpath%2Ftestuser%2Ferroreddata
```
