from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from config import AppConfig


class Database:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.is_postgres = config.is_postgres
        if not self.is_postgres:
            config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connection(self):
        if self.is_postgres:
            conn = psycopg2.connect(self.config.database_url)  # type: ignore[arg-type]
        else:
            conn = sqlite3.connect(self.config.sqlite_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON;")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _adapt_query(self, query: str) -> str:
        if self.is_postgres:
            return query
        return query.replace("%s", "?")

    def _execute(self, conn, query: str, params: tuple[Any, ...] = ()):
        if self.is_postgres:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cursor = conn.cursor()
        cursor.execute(self._adapt_query(query), params)
        return cursor

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        if row is None:
            return {}
        if isinstance(row, dict):
            return row
        return dict(row)

    def _bool_param(self, value: bool) -> bool | int:
        return value if self.is_postgres else int(value)

    def init_schema(self) -> None:
        if self.is_postgres:
            self._init_postgres_schema()
        else:
            self._init_sqlite_schema()

    def _init_postgres_schema(self) -> None:
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS websites (
                id BIGSERIAL PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                display_name TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS monitor_runs (
                id BIGSERIAL PRIMARY KEY,
                target_url TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                http_status INTEGER,
                response_time_ms INTEGER,
                snapshot_hash TEXT,
                total_items INTEGER,
                new_items_count INTEGER NOT NULL DEFAULT 0,
                raw_content_length INTEGER,
                error_message TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS new_items (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES monitor_runs(id) ON DELETE CASCADE,
                target_url TEXT NOT NULL,
                item_fingerprint TEXT NOT NULL,
                item_text TEXT NOT NULL,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                is_notified BOOLEAN NOT NULL DEFAULT FALSE,
                UNIQUE (target_url, item_fingerprint)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS email_alerts (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES monitor_runs(id) ON DELETE CASCADE,
                recipient TEXT NOT NULL,
                subject TEXT NOT NULL,
                status TEXT NOT NULL,
                sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                error_message TEXT
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_websites_active ON websites(is_active);",
            "CREATE INDEX IF NOT EXISTS idx_monitor_runs_started_at ON monitor_runs(started_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_monitor_runs_target ON monitor_runs(target_url);",
            "CREATE INDEX IF NOT EXISTS idx_new_items_detected_at ON new_items(detected_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_new_items_target ON new_items(target_url);",
        ]
        with self.connection() as conn:
            for statement in ddl:
                cursor = self._execute(conn, statement)
                cursor.close()

    def _init_sqlite_schema(self) -> None:
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS websites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                display_name TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS monitor_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_url TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT,
                http_status INTEGER,
                response_time_ms INTEGER,
                snapshot_hash TEXT,
                total_items INTEGER,
                new_items_count INTEGER NOT NULL DEFAULT 0,
                raw_content_length INTEGER,
                error_message TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS new_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES monitor_runs(id) ON DELETE CASCADE,
                target_url TEXT NOT NULL,
                item_fingerprint TEXT NOT NULL,
                item_text TEXT NOT NULL,
                detected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                is_notified INTEGER NOT NULL DEFAULT 0,
                UNIQUE (target_url, item_fingerprint)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS email_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES monitor_runs(id) ON DELETE CASCADE,
                recipient TEXT NOT NULL,
                subject TEXT NOT NULL,
                status TEXT NOT NULL,
                sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_websites_active ON websites(is_active);",
            "CREATE INDEX IF NOT EXISTS idx_monitor_runs_started_at ON monitor_runs(started_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_monitor_runs_target ON monitor_runs(target_url);",
            "CREATE INDEX IF NOT EXISTS idx_new_items_detected_at ON new_items(detected_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_new_items_target ON new_items(target_url);",
        ]
        with self.connection() as conn:
            for statement in ddl:
                cursor = self._execute(conn, statement)
                cursor.close()

    def ping(self) -> None:
        with self.connection() as conn:
            cursor = self._execute(conn, "SELECT 1;")
            cursor.fetchone()
            cursor.close()

    def add_or_activate_website(self, url: str, display_name: str | None = None) -> tuple[dict[str, Any], bool]:
        cleaned_url = url.strip()
        cleaned_name = (display_name or "").strip() or None
        if not cleaned_url:
            raise ValueError("Website URL cannot be empty.")

        with self.connection() as conn:
            existing_cursor = self._execute(
                conn,
                "SELECT * FROM websites WHERE url = %s LIMIT 1;",
                (cleaned_url,),
            )
            existing_row = existing_cursor.fetchone()
            existing_cursor.close()

            if existing_row:
                website = self._row_to_dict(existing_row)
                effective_name = cleaned_name if cleaned_name is not None else website.get("display_name")
                update_cursor = self._execute(
                    conn,
                    """
                    UPDATE websites
                    SET
                        display_name = %s,
                        is_active = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                    """,
                    (effective_name, self._bool_param(True), website["id"]),
                )
                update_cursor.close()

                refreshed_cursor = self._execute(conn, "SELECT * FROM websites WHERE id = %s;", (website["id"],))
                refreshed = self._row_to_dict(refreshed_cursor.fetchone())
                refreshed_cursor.close()
                return refreshed, False

            if self.is_postgres:
                insert_cursor = self._execute(
                    conn,
                    """
                    INSERT INTO websites (url, display_name, is_active)
                    VALUES (%s, %s, %s)
                    RETURNING *;
                    """,
                    (cleaned_url, cleaned_name, True),
                )
                inserted = self._row_to_dict(insert_cursor.fetchone())
                insert_cursor.close()
            else:
                insert_cursor = self._execute(
                    conn,
                    """
                    INSERT INTO websites (url, display_name, is_active)
                    VALUES (%s, %s, %s);
                    """,
                    (cleaned_url, cleaned_name, 1),
                )
                website_id = int(insert_cursor.lastrowid)
                insert_cursor.close()
                fetch_cursor = self._execute(conn, "SELECT * FROM websites WHERE id = %s;", (website_id,))
                inserted = self._row_to_dict(fetch_cursor.fetchone())
                fetch_cursor.close()
            return inserted, True

    def seed_websites(self, urls: list[str]) -> int:
        created_count = 0
        for url in urls:
            cleaned_url = url.strip()
            if not cleaned_url:
                continue
            _, created = self.add_or_activate_website(cleaned_url)
            if created:
                created_count += 1
        return created_count

    def get_websites(self, active_only: bool | None = True) -> list[dict[str, Any]]:
        where_clauses: list[str] = []
        params: list[Any] = []

        if active_only is True:
            where_clauses.append("is_active = %s")
            params.append(self._bool_param(True))
        elif active_only is False:
            where_clauses.append("is_active = %s")
            params.append(self._bool_param(False))

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        query = f"""
            SELECT id, url, display_name, is_active, created_at, updated_at
            FROM websites
            {where_sql}
            ORDER BY created_at ASC, id ASC;
        """

        with self.connection() as conn:
            cursor = self._execute(conn, query, tuple(params))
            rows = cursor.fetchall()
            cursor.close()
            return [self._row_to_dict(row) for row in rows]

    def get_website_by_id(self, website_id: int) -> dict[str, Any] | None:
        with self.connection() as conn:
            cursor = self._execute(conn, "SELECT * FROM websites WHERE id = %s;", (website_id,))
            row = cursor.fetchone()
            cursor.close()
            if not row:
                return None
            return self._row_to_dict(row)

    def set_website_active(self, website_id: int, is_active: bool) -> bool:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                """
                UPDATE websites
                SET is_active = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s;
                """,
                (self._bool_param(is_active), website_id),
            )
            rowcount = cursor.rowcount
            cursor.close()
            return bool(rowcount and rowcount > 0)

    def delete_website(self, website_id: int, delete_history: bool = True) -> tuple[bool, dict[str, Any] | None, int]:
        with self.connection() as conn:
            lookup_cursor = self._execute(conn, "SELECT * FROM websites WHERE id = %s LIMIT 1;", (website_id,))
            website_row = lookup_cursor.fetchone()
            lookup_cursor.close()
            if not website_row:
                return False, None, 0

            website = self._row_to_dict(website_row)
            removed_runs = 0

            if delete_history:
                runs_cursor = self._execute(
                    conn,
                    "DELETE FROM monitor_runs WHERE target_url = %s;",
                    (website["url"],),
                )
                removed_runs = int(runs_cursor.rowcount or 0)
                runs_cursor.close()

            delete_cursor = self._execute(conn, "DELETE FROM websites WHERE id = %s;", (website_id,))
            deleted = bool(delete_cursor.rowcount and delete_cursor.rowcount > 0)
            delete_cursor.close()
            return deleted, website, removed_runs

    def create_run(self, target_url: str, trigger_type: str) -> int:
        with self.connection() as conn:
            if self.is_postgres:
                cursor = self._execute(
                    conn,
                    """
                    INSERT INTO monitor_runs (target_url, trigger_type, status)
                    VALUES (%s, %s, %s)
                    RETURNING id;
                    """,
                    (target_url, trigger_type, "running"),
                )
                run_id = int(cursor.fetchone()["id"])
            else:
                cursor = self._execute(
                    conn,
                    """
                    INSERT INTO monitor_runs (target_url, trigger_type, status)
                    VALUES (%s, %s, %s);
                    """,
                    (target_url, trigger_type, "running"),
                )
                run_id = int(cursor.lastrowid)
            cursor.close()
            return run_id

    def complete_run(
        self,
        run_id: int,
        status: str,
        http_status: int | None,
        response_time_ms: int | None,
        snapshot_hash: str | None,
        total_items: int | None,
        new_items_count: int,
        raw_content_length: int | None,
        error_message: str | None,
    ) -> None:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                """
                UPDATE monitor_runs
                SET
                    status = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    http_status = %s,
                    response_time_ms = %s,
                    snapshot_hash = %s,
                    total_items = %s,
                    new_items_count = %s,
                    raw_content_length = %s,
                    error_message = %s
                WHERE id = %s;
                """,
                (
                    status,
                    http_status,
                    response_time_ms,
                    snapshot_hash,
                    total_items,
                    new_items_count,
                    raw_content_length,
                    error_message,
                    run_id,
                ),
            )
            cursor.close()

    def has_any_items(self, target_url: str) -> bool:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                "SELECT 1 FROM new_items WHERE target_url = %s LIMIT 1;",
                (target_url,),
            )
            row = cursor.fetchone()
            cursor.close()
            return bool(row)

    def insert_new_items(
        self,
        run_id: int,
        target_url: str,
        items: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        inserted: list[dict[str, str]] = []
        with self.connection() as conn:
            for item in items:
                if self.is_postgres:
                    cursor = self._execute(
                        conn,
                        """
                        INSERT INTO new_items (
                            run_id,
                            target_url,
                            item_fingerprint,
                            item_text,
                            is_notified
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (target_url, item_fingerprint) DO NOTHING
                        RETURNING id;
                        """,
                        (
                            run_id,
                            target_url,
                            item["fingerprint"],
                            item["text"],
                            False,
                        ),
                    )
                    created = cursor.fetchone()
                    if created:
                        inserted.append(item)
                else:
                    cursor = self._execute(
                        conn,
                        """
                        INSERT OR IGNORE INTO new_items (
                            run_id,
                            target_url,
                            item_fingerprint,
                            item_text,
                            is_notified
                        )
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (
                            run_id,
                            target_url,
                            item["fingerprint"],
                            item["text"],
                            0,
                        ),
                    )
                    if cursor.rowcount == 1:
                        inserted.append(item)
                cursor.close()
        return inserted

    def mark_items_notified(self, target_url: str, fingerprints: list[str]) -> None:
        if not fingerprints:
            return
        with self.connection() as conn:
            for fingerprint in fingerprints:
                cursor = self._execute(
                    conn,
                    "UPDATE new_items SET is_notified = %s WHERE target_url = %s AND item_fingerprint = %s;",
                    (self._bool_param(True), target_url, fingerprint),
                )
                cursor.close()

    def record_email_alert(
        self,
        run_id: int,
        recipient: str,
        subject: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                """
                INSERT INTO email_alerts (run_id, recipient, subject, status, error_message)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (run_id, recipient, subject, status, error_message),
            )
            cursor.close()

    def get_latest_run(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                """
                SELECT
                    r.*,
                    w.id AS website_id,
                    w.display_name AS website_display_name
                FROM monitor_runs r
                LEFT JOIN websites w ON w.url = r.target_url
                ORDER BY r.id DESC
                LIMIT 1;
                """,
            )
            row = cursor.fetchone()
            cursor.close()
            if not row:
                return None
            return self._row_to_dict(row)

    def get_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                """
                SELECT
                    r.*,
                    w.id AS website_id,
                    w.display_name AS website_display_name
                FROM monitor_runs r
                LEFT JOIN websites w ON w.url = r.target_url
                ORDER BY r.started_at DESC, r.id DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            cursor.close()
            return [self._row_to_dict(row) for row in rows]

    def get_recent_new_items(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                """
                SELECT
                    ni.id,
                    ni.run_id,
                    ni.target_url,
                    ni.item_text,
                    ni.detected_at,
                    ni.is_notified,
                    w.id AS website_id,
                    w.display_name AS website_display_name
                FROM new_items ni
                LEFT JOIN websites w ON w.url = ni.target_url
                ORDER BY ni.detected_at DESC, ni.id DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            cursor.close()
            return [self._row_to_dict(row) for row in rows]

    def get_run_by_id(self, run_id: int) -> dict[str, Any] | None:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                """
                SELECT
                    r.*,
                    w.id AS website_id,
                    w.display_name AS website_display_name
                FROM monitor_runs r
                LEFT JOIN websites w ON w.url = r.target_url
                WHERE r.id = %s;
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if not row:
                return None
            return self._row_to_dict(row)

    def get_run_items(self, run_id: int) -> list[dict[str, Any]]:
        with self.connection() as conn:
            cursor = self._execute(
                conn,
                """
                SELECT id, item_text, detected_at, is_notified
                FROM new_items
                WHERE run_id = %s
                ORDER BY id ASC;
                """,
                (run_id,),
            )
            rows = cursor.fetchall()
            cursor.close()
            return [self._row_to_dict(row) for row in rows]

    def get_runs_page(self, page: int, page_size: int) -> tuple[list[dict[str, Any]], int]:
        safe_page = max(1, page)
        safe_size = max(5, page_size)
        offset = (safe_page - 1) * safe_size
        with self.connection() as conn:
            runs_cursor = self._execute(
                conn,
                """
                SELECT
                    r.*,
                    w.id AS website_id,
                    w.display_name AS website_display_name
                FROM monitor_runs r
                LEFT JOIN websites w ON w.url = r.target_url
                ORDER BY r.started_at DESC, r.id DESC
                LIMIT %s OFFSET %s;
                """,
                (safe_size, offset),
            )
            rows = runs_cursor.fetchall()
            runs_cursor.close()

            count_cursor = self._execute(conn, "SELECT COUNT(*) AS total FROM monitor_runs;")
            count_row = self._row_to_dict(count_cursor.fetchone())
            count_cursor.close()

            total = int(count_row.get("total", 0))
            return [self._row_to_dict(row) for row in rows], total

    def get_stats(self) -> dict[str, Any]:
        with self.connection() as conn:
            runs_cursor = self._execute(
                conn,
                """
                SELECT
                    COUNT(*) AS total_checks,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS total_errors,
                    SUM(CASE WHEN status = 'new_content' THEN 1 ELSE 0 END) AS runs_with_new_content,
                    MAX(CASE WHEN status IN ('success', 'new_content', 'baseline') THEN completed_at END) AS last_success_at
                FROM monitor_runs;
                """,
            )
            run_stats = self._row_to_dict(runs_cursor.fetchone())
            runs_cursor.close()

            items_cursor = self._execute(conn, "SELECT COUNT(*) AS total_items_seen FROM new_items;")
            item_stats = self._row_to_dict(items_cursor.fetchone())
            items_cursor.close()

            websites_cursor = self._execute(
                conn,
                """
                SELECT
                    COUNT(*) AS total_websites,
                    SUM(CASE WHEN is_active = %s THEN 1 ELSE 0 END) AS active_websites
                FROM websites;
                """,
                (self._bool_param(True),),
            )
            website_stats = self._row_to_dict(websites_cursor.fetchone())
            websites_cursor.close()

            seen_sites_cursor = self._execute(
                conn,
                "SELECT COUNT(DISTINCT target_url) AS websites_with_history FROM monitor_runs;",
            )
            seen_site_stats = self._row_to_dict(seen_sites_cursor.fetchone())
            seen_sites_cursor.close()

            return {
                "total_checks": int(run_stats.get("total_checks") or 0),
                "total_errors": int(run_stats.get("total_errors") or 0),
                "runs_with_new_content": int(run_stats.get("runs_with_new_content") or 0),
                "last_success_at": run_stats.get("last_success_at"),
                "total_items_seen": int(item_stats.get("total_items_seen") or 0),
                "total_websites": int(website_stats.get("total_websites") or 0),
                "active_websites": int(website_stats.get("active_websites") or 0),
                "websites_with_history": int(seen_site_stats.get("websites_with_history") or 0),
            }
