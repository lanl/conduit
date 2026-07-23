# Copyright 2026. Triad National Security, LLC. All rights reserved.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Optional

import grpc
import certifi

from google.protobuf import any_pb2, wrappers_pb2
from google.protobuf.message import Message

from ._generated import api_pb2
from ._generated import api_pb2_grpc

from .config import ConduitClientConfig

# --- errors ---


class ConduitError(RuntimeError):
    """Base error for the client wrapper."""


class ConduitRpcError(ConduitError):
    """Raised when an underlying gRPC call fails."""

    def __init__(self, code: grpc.StatusCode, details: str):
        super().__init__(f"{code.name}: {details}")
        self.code = code
        self.details = details


# --- client wrapper ---


class ConduitClient:
    """
    Thin wrapper around the generated gRPC stub.

    - Owns channel + stub
    - Exposes convenience methods that build requests and call RPCs
    """

    def __init__(self, cfg: ConduitClientConfig):
        self._cfg = cfg
        self._channel = self._create_channel(cfg)
        self._stub = self._create_stub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> "ConduitClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ---------- Public API methods (add yours here) ----------

    def status(self, transfer_id: str, user: str = "") -> api_pb2.TransferDetails:
        """
        Returns the current status of a transfer

        "user" is only required when using a service cert. It is ignored when using a user cert
        """
        req = api_pb2.QueryOptions(queryMap={"TransferID": transfer_id}, user=user)

        try:
            resp: api_pb2.MultiTransferDetails = self._stub.Query(
                req, timeout=self._cfg.timeout_s
            )
            return resp.details.get(transfer_id)
        except grpc.RpcError as e:
            raise self._wrap_rpc_error(e) from e

    def start_transfer(
        self,
        sources: list[str],
        destination: str,
        action: str,
        user: str = "",
        options: Optional[Mapping[str, Any]] = None,
    ) -> api_pb2.TransferDetails:
        """
        Starts a CONDUIT Transfer

        "user" is only required when using a service cert. It is ignored when using a user cert
        """
        req = api_pb2.TransferRequest(
            source=sources,
            destination=destination,
            action=action,
            user=user,
        )

        for name, value in (options or {}).items():
            req.options[name].CopyFrom(_pack_any(value))

        try:
            resp: api_pb2.TransferDetails = self._stub.StartTransfer(
                req, timeout=self._cfg.timeout_s
            )
            return resp
        except grpc.RpcError as e:
            raise self._wrap_rpc_error(e) from e

    def validate_transfer(
        self,
        sources: list[str],
        destination: str,
        action: str,
        user: str = "",
        options: Optional[Mapping[str, Any]] = None,
    ) -> api_pb2.TransferDetails:
        """
        Starts a validation only transfer. No data will be transfered, it will end after the transfer gets through validation

        "user" is only required when using a service cert. It is ignored when using a user cert
        """

        req = api_pb2.TransferRequest(
            source=sources,
            destination=destination,
            action=action,
            user=user,
        )

        for name, value in (options or {}).items():
            req.options[name].CopyFrom(_pack_any(value))

        try:
            resp: api_pb2.TransferDetails = self._stub.ValidateTransfer(
                req, timeout=self._cfg.timeout_s
            )
            return resp
        except grpc.RpcError as e:
            raise self._wrap_rpc_error(e) from e

    def stop_transfer(
        self,
        transfer_id: str,
        user: str = "",
    ) -> api_pb2.TransferDetails:
        """
        Aborts a conduit transfer

        "user" is only required when using a service cert. It is ignored when using a user cert
        """

        req = api_pb2.TransferIds(
            value=[transfer_id],
            user=user,
        )

        try:
            resp: api_pb2.MultiTransferDetails = self._stub.StopTransfer(
                req, timeout=self._cfg.timeout_s
            )
            return resp.details.get(transfer_id)
        except grpc.RpcError as e:
            raise self._wrap_rpc_error(e) from e

    def watch_transfer(
        self, transfer_id: str, user: str = ""
    ) -> Iterator[api_pb2.TransferDetails]:
        """
        watch_transfer will return TransferDetails objects as the transfer progresses. It will complete when the transfer is no longer active.

        See `examples/example.py` for a usage example

        "user" is only required when using a service cert. It is ignored when using a user cert
        """

        req = api_pb2.TransferIds(
            value=[transfer_id],
            user=user,
        )

        call = self._stub.WatchStatus(req, timeout=self._cfg.timeout_s)

        try:
            for multi in call:
                # multi.details is a map[str, TransferDetails]
                details: api_pb2.TransferDetails = multi.details.get(transfer_id)
                if details is None:
                    # Not present in this update; just keep waiting.
                    continue

                yield details

                # Stop condition
                if hasattr(details, "active") and details.active is False:
                    # Cancel to close stream promptly
                    call.cancel()
                    return

        except grpc.RpcError as e:
            # If we cancelled intentionally, treat as normal completion
            if e.code() == grpc.StatusCode.CANCELLED:
                return
            raise self._wrap_rpc_error(e) from e

        finally:
            # Ensure stream is closed if we exit early for any reason
            try:
                call.cancel()
            except Exception:
                pass

    # ---------- Internals ----------

    def _create_stub(self, channel: grpc.Channel):
        return api_pb2_grpc.ConduitApiStub(channel)

    def _create_channel(self, cfg: ConduitClientConfig) -> grpc.Channel:
        """
        Creates a secure channel when TLS material is provided.
        """

        options = [
            ("grpc.max_receive_message_length", cfg.grpc_limit),
        ]

        creds = self._build_channel_credentials(cfg)
        if creds is None:
            # Insecure channel (only use if you truly want plaintext)
            return grpc.insecure_channel(cfg.addr, options=options)

        return grpc.secure_channel(cfg.addr, creds, options=options)

    def _build_channel_credentials(
        self, cfg: ConduitClientConfig
    ) -> Optional[grpc.ChannelCredentials]:
        """
        Returns ChannelCredentials if TLS should be used, otherwise None.
        Supports:
          - TLS server verification only (provide ca_pem_path)
          - mTLS (provide ca_pem_path + client cert/key)
        """
        root_certs = _read_file_bytes(certifi.where())
        if cfg.ca_pem_path:
            extra_roots = _read_file_bytes(cfg.ca_pem_path)
            root_certs = root_certs + b"\n" + extra_roots

        # mTLS: either combined PEM or separate cert+key
        client_key = None
        client_chain = None

        if cfg.cert_key_bundle_path:
            pem = _read_file_bytes(cfg.cert_key_bundle_path)
            client_key, client_chain = _split_key_and_certchain(pem)

        elif cfg.cert_pem_path and cfg.key_pem_path:
            client_chain = _read_file_bytes(cfg.cert_pem_path)
            client_key = _read_file_bytes(cfg.key_pem_path)

        # Decide if we should do TLS at all:
        # - If any of CA/cert/key is provided, we’ll do secure_channel
        if root_certs is None and client_key is None and client_chain is None:
            return None

        return grpc.ssl_channel_credentials(
            root_certificates=root_certs,
            private_key=client_key,
            certificate_chain=client_chain,
        )

    def _wrap_rpc_error(self, e: grpc.RpcError) -> ConduitRpcError:
        code = e.code() if hasattr(e, "code") else grpc.StatusCode.UNKNOWN
        details = e.details() if hasattr(e, "details") else str(e)
        return ConduitRpcError(code, details)


# --- helpers ---


def _read_file_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _split_key_and_certchain(pem: bytes) -> tuple[bytes, bytes]:
    """
    Splits a combined PEM containing a PRIVATE KEY block and one-or-more CERTIFICATE blocks.
    Returns (private_key_pem, certificate_chain_pem).
    """
    import re

    key_match = re.search(
        rb"-----BEGIN (?:RSA |EC |)PRIVATE KEY-----.*?-----END (?:RSA |EC |)PRIVATE KEY-----",
        pem,
        flags=re.DOTALL,
    )
    if not key_match:
        raise ValueError("No PRIVATE KEY block found in combined PEM")

    certs = re.findall(
        rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        pem,
        flags=re.DOTALL,
    )
    if not certs:
        raise ValueError("No CERTIFICATE blocks found in combined PEM")

    private_key = key_match.group(0) + b"\n"
    certificate_chain = b"\n".join(certs) + b"\n"
    return private_key, certificate_chain


def _pack_any(value: Any) -> any_pb2.Any:
    """
    Convert a supported Python value into google.protobuf.Any.\
    """

    if isinstance(value, any_pb2.Any):
        packed = any_pb2.Any()
        packed.CopyFrom(value)
        return packed

    # bool must be checked before int because bool subclasses int.
    if isinstance(value, bool):
        message = wrappers_pb2.BoolValue(value=value)
    elif isinstance(value, str):
        message = wrappers_pb2.StringValue(value=value)
    elif isinstance(value, bytes):
        message = wrappers_pb2.BytesValue(value=value)
    elif isinstance(value, int):
        if not -(1 << 63) <= value < (1 << 63):
            raise ValueError(f"Integer option value {value} does not fit in an int64")

        message = wrappers_pb2.Int64Value(value=value)
    elif isinstance(value, float):
        message = wrappers_pb2.DoubleValue(value=value)
    elif isinstance(value, Message):
        # Allow callers to provide their own protobuf message.
        message = value
    else:
        raise TypeError(f"Unsupported option value type: {type(value).__name__}")

    packed = any_pb2.Any()
    packed.Pack(message)
    return packed
