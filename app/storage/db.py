import json
import logging
import asyncio
import aiosqlite
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from app.core.config import settings

logger = logging.getLogger("orchestrator.db")


class Database:
    """
    Asynchronous SQLite Database Manager for NPU Orchestrator.
    Features:
      - WAL mode and high busy timeout for concurrency
      - Shared persistent connection with asyncio.Lock serialization
      - Startup recovery for orphaned/interrupted in-flight jobs
      - Automated historical job retention pruning
    """

    def __init__(self):
        self.db_path = str(settings.database_path)
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def get_connection(self) -> aiosqlite.Connection:
        """Lazily opens and returns an optimized, persistent SQLite connection."""
        if self._conn is None:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            conn = await aiosqlite.connect(self.db_path)
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA journal_mode=WAL;")
            await conn.execute("PRAGMA busy_timeout=10000;")
            await conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn = conn
        return self._conn

    async def close(self):
        """Closes the persistent SQLite connection gracefully."""
        async with self._lock:
            if self._conn is not None:
                await self._conn.close()
                self._conn = None
                logger.info("Database connection closed gracefully")

    async def init_db(self):
        """Creates tables, indices, and ensures schema compatibility."""
        conn = await self.get_connection()
        async with self._lock:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    vmid INTEGER,
                    hostname TEXT,
                    ip_address TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    logs TEXT DEFAULT '[]',
                    metadata TEXT DEFAULT '{}',
                    error TEXT
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);")
            await conn.commit()
            logger.info("Database initialized with WAL mode at %s", self.db_path)

    async def recover_orphaned_jobs(self) -> int:
        """
        Scans for in-flight jobs ('queued' or 'running') left over from a previous
        container crash or sudden restart, and transitions them to 'interrupted'.
        """
        now = datetime.now(timezone.utc).isoformat()
        conn = await self.get_connection()
        async with self._lock:
            async with conn.execute(
                "SELECT job_id, logs FROM jobs WHERE status IN ('queued', 'running')"
            ) as cursor:
                rows = await cursor.fetchall()

            recovered = 0
            for row in rows:
                job_id = row["job_id"]
                raw_logs = row["logs"]
                logs = json.loads(raw_logs) if raw_logs else []
                logs.append(f"[{now}] Job marked interrupted: Container restarted while job was in progress")
                await conn.execute(
                    """
                    UPDATE jobs 
                    SET status = 'interrupted', 
                        logs = ?, 
                        error = 'Process interrupted by container restart', 
                        updated_at = ? 
                    WHERE job_id = ?
                    """,
                    (json.dumps(logs), now, job_id)
                )
                recovered += 1

            if recovered > 0:
                await conn.commit()
                logger.warning("Recovered %d orphaned job(s) from previous container session", recovered)
            return recovered

    async def prune_old_jobs(self, days: int = 30) -> int:
        """
        Prunes terminal jobs ('completed', 'failed', 'interrupted') older than
        the retention window to keep database footprint compact and fast.
        """
        conn = await self.get_connection()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        async with self._lock:
            cursor = await conn.execute(
                """
                DELETE FROM jobs 
                WHERE status IN ('completed', 'failed', 'interrupted') 
                  AND created_at < ?
                """,
                (cutoff,)
            )
            deleted = cursor.rowcount
            if deleted > 0:
                await conn.commit()
                logger.info("Pruned %d historical job(s) older than %d days (cutoff: %s)", deleted, days, cutoff)
            return deleted

    async def create_job(
        self,
        job_id: str,
        action: str,
        metadata: Optional[Dict[str, Any]] = None,
        vmid: Optional[int] = None,
        hostname: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        init_log = [f"[{now}] Job {job_id} queued for action '{action}'"]
        conn = await self.get_connection()
        async with self._lock:
            await conn.execute(
                """
                INSERT INTO jobs (job_id, action, status, vmid, hostname, ip_address, created_at, updated_at, logs, metadata)
                VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    action,
                    vmid,
                    hostname,
                    ip_address,
                    now,
                    now,
                    json.dumps(init_log),
                    json.dumps(metadata or {}),
                ),
            )
            await conn.commit()
        return await self.get_job(job_id)

    async def append_log(self, job_id: str, message: str, status: Optional[str] = None):
        now = datetime.now(timezone.utc).isoformat()
        log_entry = f"[{now}] {message}"
        conn = await self.get_connection()
        async with self._lock:
            async with conn.execute("SELECT logs FROM jobs WHERE job_id = ?", (job_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return
                logs = json.loads(row["logs"]) if row["logs"] else []
                logs.append(log_entry)

            if status:
                await conn.execute(
                    "UPDATE jobs SET logs = ?, status = ?, updated_at = ? WHERE job_id = ?",
                    (json.dumps(logs), status, now, job_id),
                )
            else:
                await conn.execute(
                    "UPDATE jobs SET logs = ?, updated_at = ? WHERE job_id = ?",
                    (json.dumps(logs), now, job_id),
                )
            await conn.commit()

    async def update_job(
        self,
        job_id: str,
        status: str,
        vmid: Optional[int] = None,
        hostname: Optional[str] = None,
        ip_address: Optional[str] = None,
        error: Optional[str] = None,
    ):
        now = datetime.now(timezone.utc).isoformat()
        conn = await self.get_connection()
        async with self._lock:
            query = "UPDATE jobs SET status = ?, updated_at = ?"
            params = [status, now]

            if vmid is not None:
                query += ", vmid = ?"
                params.append(vmid)
            if hostname is not None:
                query += ", hostname = ?"
                params.append(hostname)
            if ip_address is not None:
                query += ", ip_address = ?"
                params.append(ip_address)
            if error is not None:
                query += ", error = ?"
                params.append(error)

            query += " WHERE job_id = ?"
            params.append(job_id)

            await conn.execute(query, tuple(params))
            await conn.commit()

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        conn = await self.get_connection()
        async with self._lock:
            async with conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                data = dict(row)
                data["logs"] = json.loads(data["logs"]) if data["logs"] else []
                data["metadata"] = json.loads(data["metadata"]) if data["metadata"] else {}
                return data

    async def list_jobs(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        conn = await self.get_connection()
        async with self._lock:
            async with conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
                result = []
                for row in rows:
                    data = dict(row)
                    data["logs"] = json.loads(data["logs"]) if data["logs"] else []
                    data["metadata"] = json.loads(data["metadata"]) if data["metadata"] else {}
                    result.append(data)
                return result


db = Database()
