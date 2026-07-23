Steps to compile protobuf:

- install protoc: https://grpc.io/docs/protoc-installation/
- install go plugins (https://grpc.io/docs/languages/go/quickstart/):
  - `go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28`
  - `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2`

```sh
protoc --go_out=. --go_opt=paths=source_relative \
--go-grpc_out=. --go-grpc_opt=paths=source_relative \
api.proto

# --grpc-web_out=import_style=typescript,mode=grpcwebtext:. \ # Typescript generation for browser support. Currently only browsers don't support grpc as they only do http1.1
```

Steps to generate python protobuf:

- install grpc tools: `python3 -m pip install -U grpcio grpcio-tools`
```sh
../clients/python/scripts/generate_pb.sh
```
