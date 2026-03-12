# client/sync_engine.py
import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from client.mysql_reader import MySQLReader, TABLE_CONFIG

logger = logging.getLogger(__name__)

MYSQL_PAGE_SIZE = int(os.getenv("MYSQL_PAGE_SIZE", "1000"))


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class TableSyncState:
    table_name:     str
    interval_secs:  int
    enabled:        bool
    last_synced_at: datetime | None = None
    last_pk:        int | None      = None
    is_initial:     bool            = True   # True until first full sync completes
    lock:           asyncio.Lock    = field(default_factory=asyncio.Lock)


@dataclass
class SyncBatch:
    table_name: str
    rows:       list[dict[str, Any]]
    page_num:   int
    is_last:    bool
    sync_state: TableSyncState


# ── Sync Engine ───────────────────────────────────────────────────────────────

class SyncEngine:
    def __init__(
        self,
        mysql_reader: MySQLReader,
        on_batch_ready,        # async callable: SyncBatch → None
        on_status_update,      # callable: str → None (updates tray tooltip)
        on_sync_complete=None, # callable: () → None (called after each table sync cycle)
    ):
        self._reader             = mysql_reader
        self._on_batch_ready     = on_batch_ready
        self._on_status          = on_status_update
        self._on_sync_complete   = on_sync_complete
        self._table_states:   dict[str, TableSyncState] = {}
        self._stop_event      = asyncio.Event()
        self._tasks:          list[asyncio.Task] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def configure(self, sync_config: list[dict]) -> None:
        """
        Called after handshake ACK with sync config from server.
        sync_config is a list of dicts:
          [{"table_name": "patient", "interval_secs": 60, "enabled": True,
            "last_synced_at": "2025-01-01T00:00:00", "last_record_id": "12345"}, ...]
        """
        for cfg in sync_config:
            table_name = cfg["table_name"]
            if table_name not in TABLE_CONFIG:
                logger.warning(f"Unknown table in sync config: {table_name} — skipping")
                continue

            last_synced_at = None
            if cfg.get("last_synced_at"):
                try:
                    last_synced_at = datetime.fromisoformat(cfg["last_synced_at"])
                    if last_synced_at.tzinfo is None:
                        last_synced_at = last_synced_at.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

            last_pk = int(cfg["last_record_id"]) if cfg.get("last_record_id") else None

            self._table_states[table_name] = TableSyncState(
                table_name    = table_name,
                interval_secs = cfg.get("interval_secs", 300),
                enabled       = cfg.get("enabled", True),
                last_synced_at= last_synced_at,
                last_pk       = last_pk,
                is_initial    = last_synced_at is None,
            )

            logger.info(
                f"Configured sync for {table_name} — "
                f"interval={cfg.get('interval_secs', 300)}s, "
                f"{'initial full sync' if last_synced_at is None else 'delta since ' + str(last_synced_at)}"
            )

    async def start(self) -> None:
        """Starts a polling task for each enabled table."""
        if not self._table_states:
            logger.warning("SyncEngine started with no tables configured")
            return

        for table_name, state in self._table_states.items():
            if not state.enabled:
                logger.info(f"Sync disabled for {table_name} — skipping")
                continue

            task = asyncio.create_task(
                self._poll_table(state),
                name=f"sync_{table_name}"
            )
            self._tasks.append(task)
            logger.info(f"Started sync task for {table_name}")

    async def stop(self) -> None:
        """Signals all polling tasks to stop."""
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("SyncEngine stopped")

    # ── Per-table polling loop ────────────────────────────────────────────────

    async def _poll_table(self, state: TableSyncState) -> None:
        """
        Polling loop for a single table.
        Runs forever until stop() is called.
        Respects the configured interval between syncs.
        """
        # Stagger startup slightly per table to avoid all tables
        # hitting MySQL at exactly the same time
        stagger = list(self._table_states.keys()).index(state.table_name) * 2
        await asyncio.sleep(stagger)

        while not self._stop_event.is_set():
            try:
                async with state.lock:
                    await self._sync_table(state)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sync error on {state.table_name}: {e}")
                # Don't crash the task — log and wait before retrying
                await asyncio.sleep(min(state.interval_secs, 30))
                continue

            # Wait for the configured interval before next sync
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=state.interval_secs
                )
            except asyncio.TimeoutError:
                pass  # interval elapsed, loop again

    # ── Sync a single table ───────────────────────────────────────────────────

    async def _sync_table(self, state: TableSyncState) -> None:
        """
        Performs one sync cycle for a table:
        - Full sync if never synced before
        - Delta sync otherwise
        Paginates results and calls on_batch_ready for each page.
        """
        table_name  = state.table_name
        since       = None if state.is_initial else state.last_synced_at
        sync_start  = datetime.now(tz=timezone.utc)

        if state.is_initial:
            row_count = await asyncio.get_event_loop().run_in_executor(
                None, self._reader.get_row_count, table_name
            )
            logger.info(
                f"Initial full sync for {table_name} — "
                f"~{row_count:,} rows to sync"
            )
            self._on_status(f"Initial sync: {table_name} (~{row_count:,} rows)")
        else:
            logger.debug(f"Delta sync for {table_name} since {since.isoformat()}")

        page_num   = 0
        total_rows = 0

        # Run the blocking MySQL paginator in a thread executor
        # so it doesn't block the asyncio event loop
        loop = asyncio.get_event_loop()

        def get_pages():
            return list(
                self._reader.fetch_changed_rows_paginated(
                    table_name=table_name,
                    since=since,
                    page_size=MYSQL_PAGE_SIZE,
                )
            )

        pages = await loop.run_in_executor(None, get_pages)

        if not pages:
            logger.debug(f"No changes found for {table_name}")
            state.last_synced_at = sync_start
            if self._on_sync_complete:
                self._on_sync_complete()
            return

        for i, page_rows in enumerate(pages):
            if self._stop_event.is_set():
                break

            is_last = (i == len(pages) - 1)
            page_num += 1
            total_rows += len(page_rows)

            batch = SyncBatch(
                table_name = table_name,
                rows       = page_rows,
                page_num   = page_num,
                is_last    = is_last,
                sync_state = state,
            )

            # Hand off to gRPC client — waits for ACK before continuing
            await self._on_batch_ready(batch)

            logger.info(
                f"{table_name} — page {page_num} sent "
                f"({len(page_rows)} rows, total={total_rows})"
            )

        # Update sync state after successful full page cycle
        state.last_synced_at = sync_start
        state.is_initial     = False
        logger.info(
            f"Sync cycle complete for {table_name} — "
            f"{total_rows} rows in {page_num} pages"
        )

        if self._on_sync_complete:
            self._on_sync_complete()