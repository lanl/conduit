# Copyright 2026. Triad National Security, LLC. All rights reserved.

from conduit import ConduitClient, ConduitClientConfig
from conduit._generated import api_pb2


def main() -> None:
    conduit_addr = "conduit-server.example.com:23456"
    ca_path = "/etc/conduit/keys/conduit-external-ca.pem"
    bundle_path = "/home/testuser/.conduit-cert-key-bundle.pem"

    cfg = ConduitClientConfig(
        addr=conduit_addr,
        timeout_s=10.0,
        ca_pem_path=ca_path,
        cert_key_bundle_path=bundle_path,
    )

    with ConduitClient(cfg) as client:
        # Start a copy transfer
        transfer: api_pb2.TransferDetails = client.start_transfer(
            sources=["/mnt/fs_1/foo/hello.txt"],
            destination="/mnt/fs_2/bar/hello.txt",
            action="CONDUIT_COPY",
            options={
                "recursive": True,
            },
        )

        print(f"Transfer ID: {transfer.transferID}")

        state: api_pb2.TransferState = api_pb2.TRANSFER_NONE

        # Watch transfer until complete
        for details in client.watch_transfer(transfer.transferID):
            if api_pb2.TransferState.Name(details.state) != state:
                state = api_pb2.TransferState.Name(details.state)
                print(f"State: {state}")
            # Loop ends automatically when transfer is no longer active

        # Get final status
        final_status = client.status(transfer.transferID)
        print(f"Final status: {final_status}")


if __name__ == "__main__":
    main()
