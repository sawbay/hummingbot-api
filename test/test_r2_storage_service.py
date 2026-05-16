from pathlib import Path

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

    def delete_object(self, Bucket, Key):
        self.deletes.append((Bucket, Key))

    def download_file(self, bucket, key, filename):
        self.downloads.append((bucket, key, filename))
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_text("downloaded", encoding="utf-8")

    def get_paginator(self, _name):
        return self

    def paginate(self, Bucket, Prefix):
        return self.pages


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
    assert service.key_for_path("instances/bot_1/conf/conf_client.yml") is None
    assert service.key_for_path("pools/bot_1/conf/conf_client.yml") is None
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
    fs_util.add_file("instances/bot_1/conf", "runtime.yml", "a: 1", override=True)

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
