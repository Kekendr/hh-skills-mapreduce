"""Общие настройки проекта: подключение к RustFS, бакет, условие варианта, пути."""
import os
from pathlib import Path

# Объектное хранилище RustFS
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "rustfsadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "rustfsadmin")
S3_REGION = "us-east-1"

BUCKET = "hh-lake"
RAW_KEY = "raw/it_vacancies_2024.csv"
RESULTS_PREFIX = "results"

# Вариант 2: «Программист, разработчик на Python»
VARIANT_ID = 2
VARIANT_NAME = "Программист, разработчик на Python"
VARIANT_ROLE = "Программист, разработчик"
VARIANT_KEYWORD = "python"

# Локальные результаты, логи и графики не входят в репозиторий, путь можно задать через окружение
REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = Path(os.environ.get("HH_RESULTS_DIR", REPO_ROOT / "results"))
FIGURES_DIR = Path(os.environ.get("HH_FIGURES_DIR", REPO_ROOT / "report" / "figures"))
LOGS_DIR = RESULTS_DIR / "logs"


class _Tee:
    """Пишет вывод одновременно в консоль и в файл лога."""

    def __init__(self, stream, path):
        self.stream, self.file = stream, open(path, "w", encoding="utf-8")

    def write(self, s):
        self.stream.write(s)
        self.file.write(s)
        return len(s)

    def flush(self):
        self.stream.flush()
        self.file.flush()


def tee_log(name):
    """Включает сохранение вывода скрипта в results/logs/<name>.txt."""
    import sys

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stdout = _Tee(sys.stdout, LOGS_DIR / f"{name}.txt")


def s3_client():
    """Создаёт boto3-клиент к RustFS с path-style адресацией."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4",
                      retries={"max_attempts": 3, "mode": "standard"}),
    )
