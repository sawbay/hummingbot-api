from pathlib import Path
import hashlib
import json

from services.r2_storage_service import R2BotsStorageService
from utils.file_system import FileSystemUtil


class FakeR2Client:
    def __init__(self):
        self.uploads = []
        self.deletes = []
        self.downloads = []
        self.pages = []

    def upload_file(self, filename, bucket, key):
        self.uploads.append((filename, bucket, key))

    def head_object(self, Bucket, Key):
        return {
            "ETag": '"uploaded-etag"',
            "ContentLength": 4,
            "LastModified": None,
        }

    def delete_object(self, Bucket, Key):
        self.deletes.append((Bucket, Key))

    def download_file(self, bucket, key, filename):
        self.downloads.append((bucket, key, filename))
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_text("downloaded", encoding="utf-8")

    def get_paginator(self, _name):
        return self

    def paginate(self, Bucket, Prefix):
        return [
            {
                **page,
                "Contents": [
                    item
                    for item in page.get("Contents", [])
                    if item.get("Key", "").startswith(Prefix)
                ],
            }
            for page in self.pages
        ]


def test_r2_key_mapping_allows_only_durable_prefixes(tmp_path):
    service = R2BotsStorageService(
        enabled=True,
        bucket="bucket",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
        prefix="bots",
        bots_root=str(tmp_path / "bots"),
        client=FakeR2Client(),
    )

    assert service.key_for_path("credentials/master_account/conf_client.yml") == (
        "bots/credentials/master_account/conf_client.yml"
    )
    assert service.key_for_path("conf/controllers/foo.yml") == "bots/conf/controllers/foo.yml"
    assert service.key_for_path("controllers/foo.py") is None
    assert service.key_for_path("scripts/foo.py") is None
    assert service.key_for_path(".gitignore") is None
    assert service.key_for_path(".dockerignore") is None
    assert service.key_for_path("instances/warmbot_1/conf/conf_client.yml") is None
    assert service.key_for_path("pools/warmbot_1/conf/conf_client.yml") is None
    assert service.key_for_path("logs/logs_hummingbot.log") is None


def test_file_system_write_through_uploads_durable_files_only(tmp_path):
    bots_root = tmp_path / "bots"
    client = FakeR2Client()
    service = R2BotsStorageService(
        enabled=True,
        bucket="bucket",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
        prefix="bots",
        bots_root=str(bots_root),
        client=client,
    )
    fs_util = FileSystemUtil(base_path=str(bots_root))
    fs_util.base_path = str(bots_root)
    fs_util.set_storage_service(service)

    fs_util.add_file("conf/controllers", "durable.yml", "a: 1", override=True)
    fs_util.add_file("controllers", "runtime.py", "print('skip')", override=True)
    fs_util.add_file("scripts", "runtime.py", "print('skip')", override=True)
    fs_util.add_file("instances/warmbot_1/conf", "runtime.yml", "a: 1", override=True)

    assert len(client.uploads) == 1
    assert client.uploads[0][2] == "bots/conf/controllers/durable.yml"

    fs_util.set_storage_service(None)


def test_background_sync_rejects_overlapping_jobs(tmp_path):
    service = R2BotsStorageService(
        enabled=False,
        bucket="",
        endpoint_url="",
        access_key_id="",
        secret_access_key="",
        bots_root=str(tmp_path / "bots"),
    )

    service.current_job = {
        "id": "existing",
        "operation": "push",
        "status": "running",
    }

    started, job = service.start_background_sync("pull")

    assert started is False
    assert job["id"] == "existing"

def test_pull_skips_unchanged_files_from_manifest(tmp_path):
    bots_root = tmp_path / "bots"
    local_file = bots_root / "conf/controllers/durable.yml"
    local_file.parent.mkdir(parents=True)
    local_file.write_text("same", encoding="utf-8")
    manifest = {
        "bots/conf/controllers/durable.yml": {
            "etag": "etag-1",
            "size": 4,
            "sha256": hashlib.sha256(b"same").hexdigest(),
            "last_modified": None,
        }
    }
    (bots_root / ".r2_sync_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    client = FakeR2Client()
    client.pages = [{
        "Contents": [
            {
                "Key": "bots/conf/controllers/durable.yml",
                "ETag": '"etag-1"',
                "Size": 4,
            }
        ]
    }]
    service = R2BotsStorageService(
        enabled=True,
        bucket="bucket",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
        prefix="bots",
        bots_root=str(bots_root),
        client=client,
    )

    result = service.pull_durable_prefixes()

    assert result.downloaded == 0
    assert result.skipped == 1
    assert client.downloads == []
    assert local_file.read_text(encoding="utf-8") == "same"

def test_pull_downloads_changed_files_and_updates_manifest(tmp_path):
    bots_root = tmp_path / "bots"
    local_file = bots_root / "conf/controllers/durable.yml"
    local_file.parent.mkdir(parents=True)
    local_file.write_text("old", encoding="utf-8")
    manifest = {
        "bots/conf/controllers/durable.yml": {
            "etag": "etag-old",
            "size": 3,
            "sha256": hashlib.sha256(b"old").hexdigest(),
            "last_modified": None,
        }
    }
    (bots_root / ".r2_sync_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    client = FakeR2Client()
    client.pages = [{
        "Contents": [
            {
                "Key": "bots/conf/controllers/durable.yml",
                "ETag": '"etag-new"',
                "Size": 10,
            }
        ]
    }]
    service = R2BotsStorageService(
        enabled=True,
        bucket="bucket",
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="key",
        secret_access_key="secret",
        prefix="bots",
        bots_root=str(bots_root),
        client=client,
    )

    result = service.pull_durable_prefixes()

    assert result.downloaded == 1
    assert len(client.downloads) == 1
    updated_manifest = json.loads((bots_root / ".r2_sync_manifest.json").read_text(encoding="utf-8"))
    assert updated_manifest["bots/conf/controllers/durable.yml"]["etag"] == "etag-new"
    assert updated_manifest["bots/conf/controllers/durable.yml"]["sha256"] == (
        hashlib.sha256(b"downloaded").hexdigest()
    )
