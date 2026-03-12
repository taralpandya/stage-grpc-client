# client/grpc_client.py
import asyncio
import logging
import os
import sys
import time
import uuid
from enum import Enum
from typing import Callable

def _resource_path(relative_path: str) -> str:
    """
    Gets the correct path to a bundled resource.
    Works both in development and when frozen by PyInstaller.
    """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

import grpc
import grpc.aio

import service_pb2
import service_pb2_grpc
from client.fingerprint import get_machine_fingerprint
from client.mysql_reader import TABLE_CONFIG

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
SERVER_HOST        = os.getenv("SERVER_HOST", "grpc-stage.phlserv.com")
SERVER_PORT        = int(os.getenv("SERVER_PORT", "443"))
CLIENT_ID          = os.getenv("CLIENT_ID")
SUBSCRIPTION_ID    = os.getenv("SUBSCRIPTION_ID")
CERT_DIR           = os.getenv("CERT_DIR", "./certs")
RECONNECT_MIN_SECS = int(os.getenv("RECONNECT_MIN_SECS", "5"))
RECONNECT_MAX_SECS = int(os.getenv("RECONNECT_MAX_SECS", "60"))


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING   = "connecting"
    CONNECTED    = "connected"
    SYNCING      = "syncing"
    RECONNECTING = "reconnecting"


class GrpcClient:
    def __init__(self, on_state_change: Callable[[ConnectionState], None]):
        self._on_state_change  = on_state_change
        self._state            = ConnectionState.DISCONNECTED
        self._last_sync_time   = None
        self._last_record_id   = ""
        self._stop_event       = asyncio.Event()
        self._channel          = None
        self._fingerprint      = get_machine_fingerprint()

        # ── Sync engine integration ───────────────────────────────────────
        self._sync_engine      = None   # set via set_sync_engine()
        self._mysql_reader     = None
        self._batch_queue      = asyncio.Queue(maxsize=10)  # backpressure
        # Maps batch_id → asyncio.Event so engine waits for ACK per batch
        self._pending_acks:    dict[str, asyncio.Event] = {}
        self._pending_results: dict[str, bool]          = {}  # batch_id → success

        logger.info(
            f"Client initialized — "
            f"ID={CLIENT_ID}, "
            f"fingerprint={self._fingerprint[:16]}..."
        )

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def last_sync_time(self):
        return self._last_sync_time

    def set_sync_engine(self, engine) -> None:
        """Wire in the SyncEngine after construction."""
        self._sync_engine = engine

    def set_mysql_reader(self, reader) -> None:
        """Wire in the MySQLReader for schema description on handshake."""
        self._mysql_reader = reader

    def notify_sync_complete(self) -> None:
        """Called by SyncEngine after each table sync cycle, even if 0 rows."""
        self._last_sync_time = time.time()
        self._state = ConnectionState.CONNECTED
        self._on_state_change(self._state)

    def stop(self):
        self._stop_event.set()

    async def run(self):
        """Main run loop — connects and reconnects with exponential backoff."""
        backoff = RECONNECT_MIN_SECS
        while not self._stop_event.is_set():
            try:
                self._set_state(ConnectionState.CONNECTING)
                await self._connect_and_stream()
                backoff = RECONNECT_MIN_SECS
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Connection error: {e}")

            if self._stop_event.is_set():
                break

            # Stop sync engine while disconnected
            if self._sync_engine:
                await self._sync_engine.stop()

            self._set_state(ConnectionState.RECONNECTING)
            logger.info(f"Reconnecting in {backoff}s...")

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass

            backoff = min(backoff * 2, RECONNECT_MAX_SECS)

        self._set_state(ConnectionState.DISCONNECTED)
        logger.info("Client stopped.")

    async def reconnect_now(self):
        """Triggered by the tray 'Reconnect' menu item."""
        if self._channel:
            await self._channel.close()

    async def enqueue_batch(self, batch) -> bool:
        """
        Called by SyncEngine when a page is ready to send.
        Blocks if the queue is full (backpressure).
        Returns True if the batch was ACK'd successfully, False if RETRY.
        """
        batch_id   = str(uuid.uuid4())
        ack_event  = asyncio.Event()
        self._pending_acks[batch_id]    = ack_event
        self._pending_results[batch_id] = False

        # Attach batch_id so the sender can reference it
        batch._batch_id = batch_id
        await self._batch_queue.put(batch)

        # Wait for server ACK before returning to SyncEngine
        await ack_event.wait()

        success = self._pending_results.pop(batch_id, False)
        self._pending_acks.pop(batch_id, None)
        return success

    # ── Connection ────────────────────────────────────────────────────────────

    async def _connect_and_stream(self):
        ca_cert     = self._read_cert("ca/ca.crt")
        client_cert = self._read_cert("client.crt")
        client_key  = self._read_cert("client.key")

        credentials = grpc.ssl_channel_credentials(
            root_certificates=ca_cert,
            private_key=client_key,
            certificate_chain=client_cert,
        )

        target = f"{SERVER_HOST}:{SERVER_PORT}"
        logger.info(f"Connecting to {target}...")

        async with grpc.aio.secure_channel(target, credentials) as channel:
            self._channel = channel
            stub          = service_pb2_grpc.DataServiceStub(channel)

            self._set_state(ConnectionState.CONNECTED)
            logger.info("Channel established — starting stream...")

            async def request_generator():
                # ── Build schemas from MySQL ──────────────────────────────────
                schemas = []
                if self._mysql_reader:
                    try:
                        raw_schemas = await asyncio.get_event_loop().run_in_executor(
                            None, self._mysql_reader.describe_tables
                        )
                        for s in raw_schemas:
                            schemas.append(service_pb2.TableSchema(
                                table_name = s["table_name"],
                                columns    = [
                                    service_pb2.ColumnSchema(
                                        column_name = col["column_name"],
                                        mysql_type  = col["mysql_type"],
                                        nullable    = col["nullable"],
                                        default_val = col["default_val"],
                                    )
                                    for col in s["columns"]
                                ]
                            ))
                        logger.info(f"Sending {len(schemas)} table schemas in handshake")
                    except Exception as e:
                        logger.error(f"Failed to describe tables for handshake: {e}")

                # ── Send handshake ────────────────────────────────────────
                yield service_pb2.ClientMessage(
                    handshake=service_pb2.Handshake(
                        client_id           = CLIENT_ID,
                        subscription_id     = SUBSCRIPTION_ID,
                        last_record_id      = self._last_record_id,
                        machine_fingerprint = self._fingerprint,
                        schemas             = schemas,
                    )
                )

                # ── Main send loop ────────────────────────────────────────
                while not self._stop_event.is_set():
                    batch_proto = await self._dequeue_batch()
                    if batch_proto:
                        yield service_pb2.ClientMessage(db_sync_batch=batch_proto)
                    else:
                        await asyncio.sleep(0.5)

            stream = stub.DataStream(request_generator())

            async for server_message in stream:
                await self._handle_server_message(server_message)

    # ── Batch dequeue ─────────────────────────────────────────────────────────

    async def _dequeue_batch(self) -> service_pb2.DbSyncBatch | None:
        """
        Pulls the next batch from the queue and converts it to a proto message.
        Returns None if nothing is ready within 0.5 seconds.
        """
        try:
            batch = await asyncio.wait_for(self._batch_queue.get(), timeout=0.5)
            self._set_state(ConnectionState.SYNCING)

            pk_col = TABLE_CONFIG.get(batch.table_name, {}).get("pk", "")

            rows = [
                service_pb2.DbRow(
                    table_name  = batch.table_name,
                    columns     = row,
                    primary_key = row.get(pk_col, ""),
                    operation   = service_pb2.RowOperation.INSERT,
                )
                for row in batch.rows
            ]

            last_pk = rows[-1].primary_key if rows else ""

            return service_pb2.DbSyncBatch(
                batch_id  = batch._batch_id,
                client_id = CLIENT_ID,
                rows      = rows,
                sync_meta = service_pb2.TableSync(
                    table_name     = batch.table_name,
                    last_record_id = last_pk,
                    last_timestamp = int(time.time()),
                    row_count      = len(rows),
                ),
                timestamp = int(time.time()),
            )

        except asyncio.TimeoutError:
            return None

    # ── Message Handlers ──────────────────────────────────────────────────────

    async def _handle_server_message(self, message: service_pb2.ServerMessage):
        kind = message.WhichOneof("payload")

        if kind == "handshake_ack":
            await self._handle_handshake_ack(message.handshake_ack)

        elif kind == "db_sync_ack":
            await self._handle_db_sync_ack(message.db_sync_ack)

        elif kind == "ack":
            ack = message.ack
            self._last_record_id = ack.last_record_id
            self._last_sync_time = time.time()
            self._set_state(ConnectionState.CONNECTED)
            logger.info(
                f"Batch {ack.batch_id} acknowledged — "
                f"status={ack.status}, last_record_id={ack.last_record_id!r}"
            )

        elif kind == "fetch_request":
            req = message.fetch_request
            logger.info(
                f"Server requesting {len(req.record_ids)} records "
                f"(force_resend={req.force_resend})"
            )

        else:
            logger.warning(f"Unknown server message type: {kind}")

    async def _handle_handshake_ack(self, ack: service_pb2.HandshakeAck):
        self._last_record_id = ack.last_record_id
        logger.info(
            f"Handshake acknowledged — "
            f"session={ack.session_id}, "
            f"resume_from={ack.last_record_id!r}, "
            f"table_configs={len(ack.sync_config)}"
        )

        # ── Start sync engine with config from server ─────────────────────
        if self._sync_engine and ack.sync_config:
            config_list = [
                {
                    "table_name":     cfg.table_name,
                    "interval_secs":  cfg.interval_secs,
                    "enabled":        cfg.enabled,
                    "last_synced_at": cfg.last_synced_at,
                    "last_record_id": cfg.last_record_id,
                }
                for cfg in ack.sync_config
            ]
            self._sync_engine.configure(config_list)
            await self._sync_engine.start()
            logger.info("SyncEngine started with server config")
        elif self._sync_engine and not ack.sync_config:
            logger.warning(
                "No sync config returned from server — "
                "check sync_config table for this client_id"
            )

    async def _handle_db_sync_ack(self, ack: service_pb2.DbSyncAck):
        """
        Server has acknowledged a DbSyncBatch.
        Signals the waiting enqueue_batch() call to unblock the SyncEngine.
        """
        self._last_sync_time = time.time()
        self._state = ConnectionState.CONNECTED  # set directly, no change check
        self._on_state_change(self._state)       # always fire so tray updates

        success = ack.status == service_pb2.AckStatus.OK


        logger.info(
            f"DbSyncAck — table={ack.table_name}, "
            f"batch={ack.batch_id}, "
            f"rows={ack.row_count}, "
            f"status={'OK' if success else 'RETRY'}"
        )

        # Unblock the SyncEngine waiting on this batch
        event = self._pending_acks.get(ack.batch_id)
        if event:
            self._pending_results[ack.batch_id] = success
            event.set()
        else:
            logger.warning(f"Received ACK for unknown batch_id: {ack.batch_id}")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _set_state(self, state: ConnectionState):
        if state != self._state:
            self._state = state
            logger.info(f"State → {state.value}")
            self._on_state_change(state)

    def _read_cert(self, filename: str) -> bytes:
        path = _resource_path(os.path.join("certs", filename))
        with open(path, "rb") as f:
            return f.read()