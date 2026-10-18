"""Этап 2. Создаёт бакет hh-lake в RustFS и загружает в него CSV с вакансиями.

Запуск: python src/upload.py [путь_к_csv]
"""
import hashlib
import sys
import time
from pathlib import Path

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from config import tee_log, BUCKET, RAW_KEY, REPO_ROOT, S3_ENDPOINT, s3_client

DEFAULT_CSV = REPO_ROOT.parent / "it_vacancies_2024.csv"


def sha256_of(path: Path) -> str:
    """Считает sha256 файла, чтобы сверить его с копией в хранилище."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main():
    """Создаёт бакет, загружает CSV и проверяет размер и хеш объекта."""
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV
    s3 = s3_client()
    print(f"Хранилище: {S3_ENDPOINT}")

    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Бакет {BUCKET} уже существует")
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Бакет {BUCKET} создан")

    size = csv_path.stat().st_size
    digest = sha256_of(csv_path)
    print(f"Локальный файл: {csv_path.name}, {size:,} байт, sha256={digest[:16]}...")

    t0 = time.perf_counter()
    s3.upload_file(
        str(csv_path), BUCKET, RAW_KEY,
        ExtraArgs={"Metadata": {"sha256": digest}, "ContentType": "text/csv"},
        Config=TransferConfig(multipart_threshold=64 << 20, multipart_chunksize=64 << 20,
                              max_concurrency=4),
    )
    dt = time.perf_counter() - t0
    print(f"Загружено за {dt:.1f} с ({size / dt / 2**20:.1f} MiB/с)")

    head = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)
    print("\nПроверка объекта в хранилище (HEAD):")
    print(f"  s3://{BUCKET}/{RAW_KEY}")
    print(f"  ContentLength = {head['ContentLength']:,} байт")
    print(f"  ETag          = {head['ETag']}")
    print(f"  LastModified  = {head['LastModified']}")
    print(f"  sha256 (meta) = {head['Metadata'].get('sha256', '')[:16]}...")
    ok = head["ContentLength"] == size and head["Metadata"].get("sha256") == digest
    print(f"  Размер и хеш совпадают с локальным файлом: {ok}")

    print(f"\nСодержимое бакета {BUCKET}:")
    for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []):
        print(f"  {obj['Size']:>14,}  {obj['Key']}")
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    tee_log("upload")
    main()
