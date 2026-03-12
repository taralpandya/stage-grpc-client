# client/tray.py
import asyncio
import logging
import os
import threading
import time
from typing import Callable

from PIL import Image
import pystray

from client.grpc_client import ConnectionState

import sys

def _resource_path(relative_path: str) -> str:
    """
    Gets the correct path to a bundled resource.
    Works both in development and when frozen by PyInstaller.
    """
    if hasattr(sys, '_MEIPASS'):
        # Running as PyInstaller bundle — files extracted to temp dir
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

logger = logging.getLogger(__name__)

ASSETS_DIR = os.getenv("ASSETS_DIR", "./assets")


def _load_image(filename: str) -> Image.Image:
    path = _resource_path(os.path.join("assets", filename))
    return Image.open(path)

class TrayApp:
    def __init__(
        self,
        on_reconnect: Callable,
        on_exit: Callable,
    ):
        self._on_reconnect    = on_reconnect
        self._on_exit         = on_exit
        self._state           = ConnectionState.DISCONNECTED
        self._last_sync_time  = None
        self._icon            = None
        self._animation_thread = None
        self._animating       = False

        # Load icons
        self._icon_connected    = _load_image("icon_connected.png")
        self._icon_disconnected = _load_image("icon_disconnected.png")
        self._icon_frames       = [
            _load_image(f"icon_syncing_{i}.png") for i in range(8)
        ]

    def run(self):
        """Start the tray icon — blocks until exit."""
        self._icon = pystray.Icon(
            name="phlserv",
            icon=self._icon_disconnected,
            title="PhlServ — Disconnected",
            menu=self._build_menu(),
        )
        self._icon.run()

    def update_state(self, state: ConnectionState, last_sync_time=None):
        """Called by the gRPC client when connection state changes."""
        self._state          = state
        self._last_sync_time = last_sync_time

        if self._icon is None:
            return

        if state == ConnectionState.SYNCING:
            self._start_animation()
        else:
            self._stop_animation()
            if state == ConnectionState.CONNECTED:
                self._icon.icon  = self._icon_connected
                self._icon.title = "PhlServ — Connected"
            elif state in (ConnectionState.DISCONNECTED, ConnectionState.RECONNECTING):
                self._icon.icon  = self._icon_disconnected
                self._icon.title = f"PhlServ — {state.value.capitalize()}"
            elif state == ConnectionState.CONNECTING:
                self._icon.icon  = self._icon_disconnected
                self._icon.title = "PhlServ — Connecting..."

        # Rebuild menu to refresh status/last sync time
        self._icon.menu = self._build_menu()

    # ── Menu ──────────────────────────────────────────────────────────────────

    def _build_menu(self) -> pystray.Menu:
        # Connection status label
        status_label = f"Status: {self._state.value.capitalize()}"

        # Last sync time label
        if self._last_sync_time:
            sync_str = time.strftime("%H:%M:%S", time.localtime(self._last_sync_time))
            sync_label = f"Last sync: {sync_str}"
        else:
            sync_label = "Last sync: Never"

        return pystray.Menu(
            pystray.MenuItem(status_label, None, enabled=False),
            pystray.MenuItem(sync_label,   None, enabled=False),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Reconnect", self._on_reconnect_clicked),
            pystray.MenuItem("View Logs", self._on_view_logs_clicked),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Exit", self._on_exit_clicked),
        )

    def _on_reconnect_clicked(self, icon, item):
        logger.info("Manual reconnect requested from tray")
        asyncio.run_coroutine_threadsafe(
            self._on_reconnect(),
            asyncio.get_event_loop(),
        )

    def _on_view_logs_clicked(self, icon, item):
        log_path = os.path.abspath("phlserv-client.log")
        os.startfile(log_path)  # Windows only — opens log in default text editor

    def _on_exit_clicked(self, icon, item):
        logger.info("Exit requested from tray")
        self._stop_animation()
        self._icon.stop()
        self._on_exit()

    # ── Animation ─────────────────────────────────────────────────────────────

    def _start_animation(self):
        if self._animating:
            return
        self._animating = True
        self._animation_thread = threading.Thread(
            target=self._animate,
            daemon=True
        )
        self._animation_thread.start()

    def update_tooltip(self, msg: str):
        """Update the tray tooltip with a custom status message."""
        if self._icon:
            self._icon.title = f"PhlServ — {msg}"

    def _stop_animation(self):
        self._animating = False

    def _animate(self):
        frame = 0
        while self._animating and self._icon:
            self._icon.icon  = self._icon_frames[frame % len(self._icon_frames)]
            self._icon.title = "PhlServ — Syncing..."
            frame += 1
            time.sleep(0.1)  # 10 fps animation