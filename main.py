# main.py
import os
import sys

def _resource_path(relative_path: str) -> str:
    """
    Gets the correct path to a bundled resource.
    Works both in development and when frozen by PyInstaller.
    """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

from dotenv import load_dotenv
load_dotenv(_resource_path(".env"))

import asyncio
import logging
import threading

from client.fingerprint import get_machine_fingerprint
from client.grpc_client import GrpcClient, ConnectionState
from client.mysql_reader import MySQLReader
from client.sync_engine import SyncEngine
from client.tray import TrayApp

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("phlserv-client.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # ── Validate required env vars ────────────────────────────────────────────
    for var in ("CLIENT_ID", "SUBSCRIPTION_ID", "MYSQL_USER", "MYSQL_PASSWORD"):
        if not os.getenv(var):
            logger.error(f"Required env var {var} is not set — exiting")
            return

    # ── Build components ──────────────────────────────────────────────────────
    tray         = None
    grpc_client  = None
    mysql_reader = None
    sync_engine  = None

    def on_state_change(state: ConnectionState):
        if tray is not None and grpc_client is not None:
            tray.update_state(state, grpc_client.last_sync_time)

    def on_status_update(msg: str):
        if tray:
            tray.update_tooltip(msg)

    def on_reconnect():
        if grpc_client:
            asyncio.run_coroutine_threadsafe(grpc_client.reconnect_now(), loop)

    def on_exit():
        logger.info("Shutting down...")

        if sync_engine:
            asyncio.run_coroutine_threadsafe(sync_engine.stop(), loop)

        if grpc_client:
            grpc_client.stop()

        if mysql_reader:
            mysql_reader.disconnect()

        loop.call_soon_threadsafe(loop.stop)

    # ── MySQL reader ──────────────────────────────────────────────────────────
    try:
        mysql_reader = MySQLReader()
        mysql_reader.connect()
        logger.info("MySQL connection established")
    except Exception as e:
        logger.error(f"Failed to connect to OpenDental MySQL: {e}")
        logger.error("Check MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE in .env")
        return

    # ── gRPC client ───────────────────────────────────────────────────────────
    grpc_client = GrpcClient(on_state_change=on_state_change)

    # ── Sync engine ───────────────────────────────────────────────────────────
    sync_engine = SyncEngine(
        mysql_reader      = mysql_reader,
        on_batch_ready    = grpc_client.enqueue_batch,
        on_status_update  = on_status_update,
        on_sync_complete  = grpc_client.notify_sync_complete,
    )

    # Wire sync engine into gRPC client
    grpc_client.set_sync_engine(sync_engine)
    grpc_client.set_mysql_reader(mysql_reader)

    # ── Tray ──────────────────────────────────────────────────────────────────
    tray = TrayApp(on_reconnect=on_reconnect, on_exit=on_exit)

    # ── Run gRPC client in background thread ──────────────────────────────────
    def run_grpc():
        try:
            loop.run_until_complete(grpc_client.run())
        except Exception as e:
            logger.error(f"gRPC client crashed: {e}")

    grpc_thread = threading.Thread(target=run_grpc, daemon=True, name="grpc-client")
    grpc_thread.start()

    # ── Run tray in main thread (required by Windows) ─────────────────────────
    logger.info(
        f"PhlServ client starting — "
        f"client_id={os.getenv('CLIENT_ID')}, "
        f"server={os.getenv('SERVER_HOST', 'grpc-stage.phlserv.com')}"
    )
    tray.run()  # blocks until exit

    # ── Cleanup after tray exits ──────────────────────────────────────────────
    grpc_thread.join(timeout=5)
    logger.info("PhlServ client exited cleanly")


if __name__ == "__main__":
    main()