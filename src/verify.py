"""Этап 4. Сверка результатов MapReduce и DuckDB по каждому сочетанию навыков.

Оба результата скачиваются из бакета и сравниваются по ключам и частотам,
для каждого независимо считается контрольный sha256.

Запуск: python src/verify.py [engine_a] [engine_b]
"""
import csv
import hashlib
import io
import sys

from config import BUCKET, RESULTS_PREFIX, s3_client, tee_log

NAMES = {1: "skills", 2: "pairs", 3: "triples"}


def load(s3, engine, k):
    """Скачивает результат движка для размера k и разбирает его в словарь."""
    key = f"{RESULTS_PREFIX}/{engine}/k{k}_{NAMES[k]}.csv"
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
    rows = csv.reader(io.StringIO(body))
    next(rows)
    return {tuple(r[:-1]): int(r[-1]) for r in rows}, len(body.encode("utf-8"))


def digest(results):
    """Считает sha256 результата так же, как MapReduce, но по скачанным файлам."""
    h = hashlib.sha256()
    for k in (1, 2, 3):
        for key in sorted(results[k]):
            h.update(f"{k}\t{chr(31).join(key)}\t{results[k][key]}\n".encode("utf-8"))
    return h.hexdigest()


def main():
    """Сравнивает два результата и печатает итог сверки."""
    a, b = (sys.argv[1:3] + ["mapreduce", "duckdb"][len(sys.argv[1:3]):])[:2]
    tee_log({("mapreduce", "duckdb"): "verify",
             ("mapreduce_optimized", "duckdb_parquet"): "verify_optimized",
             ("mapreduce", "mapreduce_failtest"): "verify_failtest"}.get((a, b), f"verify_{a}_{b}"))
    s3 = s3_client()
    ra, rb = {}, {}
    print(f"Сверка: {a}  vs  {b}  (файлы читаются из s3://{BUCKET}/{RESULTS_PREFIX}/)\n")
    print(f"{'k':>2} | {'уникальных ' + a:>22} | {'уникальных ' + b:>22} | {'сумма ' + a:>16} | "
          f"{'сумма ' + b:>16} | {'ключи равны':>11} | {'частоты равны':>13} | расхождений")
    all_ok = True
    for k in (1, 2, 3):
        ra[k], _ = load(s3, a, k)
        rb[k], _ = load(s3, b, k)
        same_keys = ra[k].keys() == rb[k].keys()
        diff = [key for key in ra[k].keys() | rb[k].keys() if ra[k].get(key) != rb[k].get(key)]
        ok = same_keys and not diff
        all_ok &= ok
        print(f"{k:>2} | {len(ra[k]):>22,} | {len(rb[k]):>22,} | {sum(ra[k].values()):>16,} | "
              f"{sum(rb[k].values()):>16,} | {str(same_keys):>11} | {str(not diff):>13} | {len(diff)}")
        for key in diff[:5]:
            print(f"     расхождение: {key}: {a}={ra[k].get(key)} {b}={rb[k].get(key)}")

    da, db = digest(ra), digest(rb)
    print(f"\nsha256 {a:<10}: {da}")
    print(f"sha256 {b:<10}: {db}")
    print(f"\nКонтрольные сочетания (частота {a} / {b}):")
    for k, key in [(1, ("python",)), (1, ("django",)), (2, ("fastapi", "python")),
                   (3, ("docker", "postgresql", "python"))]:
        print(f"  {' + '.join(key):<32} {ra[k].get(key)} / {rb[k].get(key)}")
    verdict = all_ok and da == db
    print(f"\nИТОГ: результаты {'ПОЛНОСТЬЮ СОВПАДАЮТ' if verdict else 'РАСХОДЯТСЯ'}")
    sys.exit(0 if verdict else 1)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
