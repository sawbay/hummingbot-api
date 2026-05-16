import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import boto3

logger = logging.getLogger(__name__)


@dataclass
class R2SyncResult:
    operation: str
    uploaded: int = 0
    downloaded: int = 0
    deleted: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict:
        return {
            "operation": self.operation,
            "success": self.success,
            "uploaded": self.uploaded,
            "downloaded": self.downloaded,
            "deleted": self.deleted,
            "skipped": self.skipped,
            "errors": self.errors,
        }


class R2BotsStorageService:
    """S3-compatible storage service for durable files under bots/."""

    DURABLE_PREFIXES = ("credentials", "conf", "controllers", "scripts")
    EXCLUDED_PREFIXES = ("instances", "pools", "archived", "data", "logs")

    def __init__(
        self,
        enabled: bool,
        bucket: str,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        prefix: str = "bots",
        bots_root: str = "bots",
        client=None,
    ):
        self.enabled = enabled
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.prefix = prefix.strip("/")
        self.bots_root = Path(bots_root)
        self._client = client
        self.last_sync_result: dict | None = None
        self.current_job: dict | None = None
        self._job_lock = threading.Lock()

        if self.enabled and self._client is None:
            self._validate_config()
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )

    def _validate_config(self) -> None:
        missing = [
            name
            for name, value in {
                "R2_BUCKET": self.bucket,
                "R2_ENDPOINT_URL": self.endpoint_url,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"R2 is enabled but missing required settings: {', '.join(missing)}")

    @property
    def client(self):
        if not self._client:
            raise RuntimeError("R2 storage service is disabled")
        return self._client

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "bucket": self.bucket if self.enabled else None,
            "endpoint_url": self.endpoint_url if self.enabled else None,
            "prefix": self.prefix,
            "bots_root": str(self.bots_root),
            "durable_prefixes": list(self.DURABLE_PREFIXES),
            "excluded_prefixes": list(self.EXCLUDED_PREFIXES),
            "last_sync_result": self.last_sync_result,
            "current_job": self.current_job,
        }

    def start_background_sync(self, operation: str) -> tuple[bool, dict]:
        if operation not in ("pull", "push"):
            raise ValueError(f"Unsupported R2 sync operation: {operation}")

        with self._job_lock:
            if self.current_job and self.current_job.get("status") == "running":
                return False, self.current_job.copy()

            job = {
                "id": str(uuid.uuid4()),
                "operation": operation,
                "status": "running",
                "started_at": time.time(),
                "finished_at": None,
                "result": None,
                "error": None,
            }
            self.current_job = job

        thread = threading.Thread(target=self._run_background_sync, args=(job["id"], operation), daemon=True)
        thread.start()
        return True, job.copy()

    def _run_background_sync(self, job_id: str, operation: str) -> None:
        logger.info("Started R2 %s job %s", operation, job_id)
        try:
            result = self.pull_durable_prefixes() if operation == "pull" else self.push_durable_prefixes()
            status = "completed" if result.success else "failed"
            error = None if result.success else "; ".join(result.errors)
            result_dict = result.to_dict()
        except Exception as exc:
            logger.error("R2 %s job %s failed: %s", operation, job_id, exc, exc_info=True)
            status = "failed"
            error = str(exc)
            result_dict = None

        with self._job_lock:
            if self.current_job and self.current_job.get("id") == job_id:
                self.current_job.update({
                    "status": status,
                    "finished_at": time.time(),
                    "result": result_dict,
                    "error": error,
                })
        logger.info("Finished R2 %s job %s with status=%s", operation, job_id, status)

    def is_durable_path(self, path: str | os.PathLike) -> bool:
        relative = self.to_relative_path(path)
        if not relative:
            return False
        first = relative.parts[0]
        return first in self.DURABLE_PREFIXES and first not in self.EXCLUDED_PREFIXES

    def to_relative_path(self, path: str | os.PathLike) -> Path | None:
        candidate = Path(path)
        if candidate.is_absolute():
            try:
                candidate = candidate.resolve().relative_to(self.bots_root.resolve())
            except ValueError:
                return None
        elif candidate.parts and candidate.parts[0] == self.bots_root.name:
            candidate = Path(*candidate.parts[1:])

        if any(part in ("", ".", "..") for part in candidate.parts):
            return None
        return candidate

    def key_for_path(self, path: str | os.PathLike) -> str | None:
        relative = self.to_relative_path(path)
        if relative is None or not self.is_durable_path(relative):
            return None
        key_path = relative.as_posix()
        return f"{self.prefix}/{key_path}" if self.prefix else key_path

    def upload_file(self, path: str | os.PathLike) -> bool:
        if not self.enabled:
            return False
        key = self.key_for_path(path)
        if not key:
            return False
        full_path = self._full_local_path(path)
        if not full_path.is_file():
            return False
        self.client.upload_file(str(full_path), self.bucket, key)
        logger.info("Uploaded durable bots file to R2: %s -> %s", full_path, key)
        return True

    def upload_tree(self, path: str | os.PathLike) -> R2SyncResult:
        result = R2SyncResult(operation="upload_tree")
        full_path = self._full_local_path(path)
        if full_path.is_file():
            result.uploaded += int(self.upload_file(full_path))
            return result
        if not full_path.exists():
            result.skipped += 1
            return result
        for file_path in self._iter_files(full_path):
            try:
                if self.upload_file(file_path):
                    result.uploaded += 1
                else:
                    result.skipped += 1
            except Exception as exc:
                result.errors.append(f"{file_path}: {exc}")
        self.last_sync_result = result.to_dict()
        return result

    def delete_path(self, path: str | os.PathLike) -> bool:
        if not self.enabled:
            return False
        key = self.key_for_path(path)
        if not key:
            return False
        self.client.delete_object(Bucket=self.bucket, Key=key)
        logger.info("Deleted durable bots object from R2: %s", key)
        return True

    def delete_tree(self, path: str | os.PathLike) -> R2SyncResult:
        result = R2SyncResult(operation="delete_tree")
        relative = self.to_relative_path(path)
        if relative is None or not self.is_durable_path(relative):
            result.skipped += 1
            return result
        prefix = self._object_prefix(relative.as_posix())
        for key in self.list_keys(prefix):
            try:
                self.client.delete_object(Bucket=self.bucket, Key=key)
                result.deleted += 1
            except Exception as exc:
                result.errors.append(f"{key}: {exc}")
        self.last_sync_result = result.to_dict()
        return result

    def pull_durable_prefixes(self) -> R2SyncResult:
        result = R2SyncResult(operation="pull")
        if not self.enabled:
            result.skipped += 1
            self.last_sync_result = result.to_dict()
            return result
        self.ensure_local_directories()
        for durable_prefix in self.DURABLE_PREFIXES:
            object_prefix = self._object_prefix(durable_prefix)
            for key in self.list_keys(object_prefix):
                relative = self._relative_from_key(key)
                if relative is None or not self.is_durable_path(relative):
                    result.skipped += 1
                    continue
                destination = self.bots_root / relative
                try:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    self.client.download_file(self.bucket, key, str(destination))
                    result.downloaded += 1
                except Exception as exc:
                    result.errors.append(f"{key}: {exc}")
        self.last_sync_result = result.to_dict()
        return result

    def push_durable_prefixes(self) -> R2SyncResult:
        result = R2SyncResult(operation="push")
        if not self.enabled:
            result.skipped += 1
            self.last_sync_result = result.to_dict()
            return result
        self.ensure_local_directories()
        for durable_prefix in self.DURABLE_PREFIXES:
            local_prefix = self.bots_root / durable_prefix
            if not local_prefix.exists():
                result.skipped += 1
                continue
            subtree_result = self.upload_tree(local_prefix)
            result.uploaded += subtree_result.uploaded
            result.skipped += subtree_result.skipped
            result.errors.extend(subtree_result.errors)
        self.last_sync_result = result.to_dict()
        return result

    def list_keys(self, prefix: str) -> Iterable[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if key and not key.endswith("/"):
                    yield key

    def ensure_local_directories(self) -> None:
        for prefix in (*self.DURABLE_PREFIXES, *self.EXCLUDED_PREFIXES):
            (self.bots_root / prefix).mkdir(parents=True, exist_ok=True)

    def _full_local_path(self, path: str | os.PathLike) -> Path:
        candidate = Path(path)
        if candidate.is_absolute():
            return candidate
        if candidate.parts and candidate.parts[0] == self.bots_root.name:
            return candidate
        return self.bots_root / candidate

    def _iter_files(self, root: Path):
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [
                dirname
                for dirname in dirnames
                if not self._is_excluded_relative(Path(dirpath) / dirname)
            ]
            for filename in filenames:
                yield Path(dirpath) / filename

    def _is_excluded_relative(self, path: Path) -> bool:
        relative = self.to_relative_path(path)
        return bool(relative and relative.parts and relative.parts[0] in self.EXCLUDED_PREFIXES)

    def _object_prefix(self, relative_prefix: str) -> str:
        clean = relative_prefix.strip("/")
        key = f"{self.prefix}/{clean}" if self.prefix else clean
        return f"{key.rstrip('/')}/" if clean else key

    def _relative_from_key(self, key: str) -> Path | None:
        key_prefix = f"{self.prefix}/" if self.prefix else ""
        if key_prefix and not key.startswith(key_prefix):
            return None
        return self.to_relative_path(key[len(key_prefix):])
