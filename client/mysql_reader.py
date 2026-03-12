# client/mysql_reader.py
import logging
import os
from datetime import datetime
from typing import Any, Generator

import mysql.connector
from mysql.connector import Error as MySQLError

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
MYSQL_HOST      = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT      = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER      = os.getenv("MYSQL_USER")
MYSQL_PASSWORD  = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE  = os.getenv("MYSQL_DATABASE", "opendental")
PAGE_SIZE       = int(os.getenv("MYSQL_PAGE_SIZE", "1000"))

# ── Table definitions ─────────────────────────────────────────────────────────
TABLE_CONFIG = {
    "patient":      {"pk": "PatNum",  "timestamp_col": "DateTStamp"},
    "appointment":  {"pk": "AptNum",  "timestamp_col": "DateTStamp"},
    "procedurelog": {"pk": "ProcNum", "timestamp_col": "DateTStamp"},
}

EXCLUDED_COLUMNS = {
    "patient": {"SecurityHash"},
}


class MySQLReader:
    def __init__(self):
        self._connection = None

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        try:
            self._connection = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                connection_timeout=10,
                autocommit=True,
                use_pure=True,
            )
            logger.info(f"Connected to OpenDental MySQL at {MYSQL_HOST}:{MYSQL_PORT}")
        except MySQLError as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise

    def describe_tables(self) -> list[dict]:
        """
        Runs DESCRIBE on each configured table and returns schema metadata.
        Returns a list of dicts ready to be sent in the Handshake proto.
        """
        self.ensure_connected()
        schemas = []

        for table_name in TABLE_CONFIG.keys():
            try:
                cursor = self._connection.cursor(dictionary=True)
                cursor.execute(f"DESCRIBE `{table_name}`")
                rows = cursor.fetchall()
                cursor.close()

                columns = [
                    {
                        "column_name": row["Field"],
                        "mysql_type":  row["Type"],
                        "nullable":    row["Null"] == "YES",
                        "default_val": str(row["Default"]) if row["Default"] is not None else "",
                    }
                    for row in rows
                ]
                schemas.append({
                    "table_name": table_name,
                    "columns":    columns,
                })
                logger.info(f"Described {table_name} — {len(columns)} columns")

            except Exception as e:
                logger.error(f"Failed to describe {table_name}: {e}")

        return schemas

    def disconnect(self) -> None:
        if self._connection and self._connection.is_connected():
            self._connection.close()
            logger.info("MySQL connection closed")

    def is_connected(self) -> bool:
        try:
            if self._connection and self._connection.is_connected():
                self._connection.ping(reconnect=True, attempts=3, delay=1)
                return True
        except Exception:
            pass
        return False

    def ensure_connected(self) -> None:
        if not self.is_connected():
            logger.warning("MySQL connection lost — reconnecting...")
            self.connect()

    # ── Paginated fetching ────────────────────────────────────────────────────

    def fetch_changed_rows_paginated(
        self,
        table_name: str,
        since: datetime | None,
        page_size: int = PAGE_SIZE,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """
        Generator that yields pages of changed rows.
        Each page is a list of dicts of up to page_size rows.

        Usage:
            for page in reader.fetch_changed_rows_paginated("patient", since):
                # send page via gRPC
                # wait for ACK
                # then next page is fetched

        For initial full sync (since=None): paginates by PK offset
        For delta sync (since=timestamp):   paginates by timestamp + PK
        """
        self.ensure_connected()

        config   = TABLE_CONFIG[table_name]
        pk_col   = config["pk"]
        ts_col   = config["timestamp_col"]
        excluded = EXCLUDED_COLUMNS.get(table_name, set())
        offset   = 0

        logger.info(
            f"Starting paginated sync on {table_name} — "
            f"{'full' if since is None else 'delta since ' + since.isoformat()} "
            f"(page_size={page_size})"
        )

        while True:
            self.ensure_connected()

            try:
                cursor = self._connection.cursor(dictionary=True)

                if since is None:
                    # ── Full sync — paginate by LIMIT/OFFSET ──────────────
                    # OFFSET is safe here because we're doing a one-time
                    # full sync ordered by PK — data won't shift under us
                    query = f"""
                        SELECT * FROM `{table_name}`
                        ORDER BY `{pk_col}` ASC
                        LIMIT %s OFFSET %s
                    """
                    cursor.execute(query, (page_size, offset))

                else:
                    # ── Delta sync — paginate by PK after last seen ───────
                    # Using PK offset rather than LIMIT/OFFSET avoids the
                    # "offset drift" problem where new rows shift pages
                    if offset == 0:
                        query = f"""
                            SELECT * FROM `{table_name}`
                            WHERE `{ts_col}` > %s
                            ORDER BY `{ts_col}` ASC, `{pk_col}` ASC
                            LIMIT %s
                        """
                        cursor.execute(query, (since, page_size))
                    else:
                        query = f"""
                            SELECT * FROM `{table_name}`
                            WHERE `{ts_col}` > %s
                            AND `{pk_col}` > %s
                            ORDER BY `{ts_col}` ASC, `{pk_col}` ASC
                            LIMIT %s
                        """
                        cursor.execute(query, (since, last_pk, page_size))

                rows = cursor.fetchall()
                cursor.close()

                if not rows:
                    logger.info(f"Paginated sync complete for {table_name} — {offset} rows total")
                    break

                # Serialize and clean
                clean_rows = [
                    {
                        k: self._serialize_value(v)
                        for k, v in row.items()
                        if k not in excluded
                    }
                    for row in rows
                ]

                last_pk = rows[-1][pk_col]
                offset += len(rows)

                logger.info(
                    f"{table_name} — page yielded: {len(clean_rows)} rows "
                    f"(total so far: {offset}, last_pk={last_pk})"
                )

                yield clean_rows

                # If we got fewer rows than page_size we're done
                if len(rows) < page_size:
                    logger.info(f"Paginated sync complete for {table_name} — {offset} rows total")
                    break

            except MySQLError as e:
                logger.error(f"Failed to fetch page from {table_name}: {e}")
                raise

    def fetch_rows_by_pk(
        self,
        table_name: str,
        pk_values: list[int],
        page_size: int = PAGE_SIZE,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """
        Fetches specific rows by primary key, paginated.
        Used when server sends a FetchRequest for specific records.
        """
        if not pk_values:
            return

        self.ensure_connected()

        config   = TABLE_CONFIG[table_name]
        pk_col   = config["pk"]
        excluded = EXCLUDED_COLUMNS.get(table_name, set())

        # Split pk_values into pages
        for i in range(0, len(pk_values), page_size):
            page_pks = pk_values[i:i + page_size]
            try:
                cursor       = self._connection.cursor(dictionary=True)
                placeholders = ",".join(["%s"] * len(page_pks))
                query = f"""
                    SELECT * FROM `{table_name}`
                    WHERE `{pk_col}` IN ({placeholders})
                    ORDER BY `{pk_col}` ASC
                """
                cursor.execute(query, page_pks)
                rows = cursor.fetchall()
                cursor.close()

                clean_rows = [
                    {
                        k: self._serialize_value(v)
                        for k, v in row.items()
                        if k not in excluded
                    }
                    for row in rows
                ]

                logger.info(
                    f"Fetched {len(clean_rows)} rows by PK from {table_name} "
                    f"(page {i // page_size + 1})"
                )

                yield clean_rows

            except MySQLError as e:
                logger.error(f"Failed to fetch by PK from {table_name}: {e}")
                raise

    def get_row_count(self, table_name: str) -> int:
        """Returns total row count — useful for progress tracking during initial sync."""
        self.ensure_connected()
        try:
            cursor = self._connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except MySQLError as e:
            logger.error(f"Failed to get row count for {table_name}: {e}")
            return 0

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _serialize_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")
        return str(value)