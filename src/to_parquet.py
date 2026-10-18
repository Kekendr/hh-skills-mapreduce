"""Бонус. Однократно конвертирует исходный CSV в Parquet в зоне curated/ бакета.

Parquet хранит данные по колонкам со сжатием, поэтому запрос читает из хранилища
только нужные колонки, а не весь CSV.

Запуск: python src/to_parquet.py
"""
import sys
import time

from config import tee_log, BUCKET, RAW_KEY, s3_client
from duckdb_counts import PARQUET_KEY, connect, source_expr


def main():
    """Конвертирует CSV в Parquet и сравнивает размеры."""
    con = connect()
    t0 = time.perf_counter()
    con.execute(f"""COPY (SELECT * FROM {source_expr('csv')})
                    TO 's3://{BUCKET}/{PARQUET_KEY}' (FORMAT parquet, COMPRESSION zstd)""")
    dt = time.perf_counter() - t0
    s3 = s3_client()
    csv_size = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)["ContentLength"]
    pq_size = s3.head_object(Bucket=BUCKET, Key=PARQUET_KEY)["ContentLength"]
    rows = con.execute(f"SELECT count(*) FROM read_parquet('s3://{BUCKET}/{PARQUET_KEY}')").fetchone()[0]
    print(f"s3://{BUCKET}/{RAW_KEY}: {csv_size:,} байт")
    print(f"s3://{BUCKET}/{PARQUET_KEY}: {pq_size:,} байт ({pq_size / csv_size:.1%} от CSV), строк {rows:,}")
    print(f"Конвертация: {dt:.1f} с (выполняется один раз)")


if __name__ == "__main__":
    tee_log("to_parquet")
    main()
