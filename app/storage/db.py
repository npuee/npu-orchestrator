import json
import logging
import aiosqlite
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from app.core.config import settings

logger = logging.getLogger("orchestrator.db")


class Database:
    def __init__(self):
        self.db_path = str(settings.database_path)

    async def init_db(self):
        """Creates tables and enables WAL mode."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("""
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
            await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);")
            await db.commit()
            logger.info("Database initialized at %s", self.db_path)

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
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
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
            await db.commit()
        return await self.get_job(job_id)

    async def append_log(self, job_id: str, message: str, status: Optional[str] = None):
        now = datetime.now(timezone.utc).isoformat()
        log_entry = f"[{now}] {message}"
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT logs FROM jobs WHERE job_id = ?", (job_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return
                logs = json.loads(row[0]) if row[0] else []
                logs.append(log_entry)
            
            if status:
                await db.execute(
                    "UPDATE jobs SET logs = ?, status = ?, updated_at = ? WHERE job_id = ?",
                    (json.dumps(logs), status, now, job_id),
                )
            else:
                await db.execute(
                    "UPDATE jobs SET logs = ?, updated_at = ? WHERE job_id = ?",
                    (json.dumps(logs), now, job_id),
                )
            await db.commit()

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
        async with aiosqlite.connect(self.db_path) as db:
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

            await db.execute(query, tuple(params))
            await db.commit()

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                data = dict(row)
                data["logs"] = json.loads(data["logs"]) if data["logs"] else []
                data["metadata"] = json.loads(data["metadata"]) if data["metadata"] else {}
                return data

    async def list_jobs(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
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
