import argparse
import json

from config import settings
from services.r2_storage_service import R2BotsStorageService


def build_storage_service() -> R2BotsStorageService:
    return R2BotsStorageService(
        enabled=settings.r2.enabled,
        bucket=settings.r2.bucket,
        endpoint_url=settings.r2.endpoint_url,
        access_key_id=settings.r2.access_key_id,
        secret_access_key=settings.r2.secret_access_key,
        prefix=settings.r2.prefix,
        bots_root="bots",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync durable bots files with Cloudflare R2.")
    parser.add_argument("operation", choices=("push", "pull", "status"))
    args = parser.parse_args()

    storage_service = build_storage_service()
    if args.operation == "push":
        result = storage_service.push_durable_prefixes().to_dict()
    elif args.operation == "pull":
        result = storage_service.pull_durable_prefixes().to_dict()
    else:
        result = storage_service.status()

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
