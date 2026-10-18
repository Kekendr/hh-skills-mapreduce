"""Этап 3б. Частоты навыков, пар и троек для варианта 2 SQL-запросом DuckDB.

DuckDB читает тот же CSV прямо из бакета hh-lake через расширение httpfs и записывает
результаты обратно в бакет командой COPY ... TO 's3://...'.

Запуск: python src/duckdb_counts.py [--source csv|parquet] [--no-write]
"""
import argparse
import json
import sys
import time
from urllib.parse import urlparse

import duckdb

from config import (BUCKET, tee_log, RAW_KEY, RESULTS_DIR, RESULTS_PREFIX, S3_ACCESS_KEY, S3_ENDPOINT,
                    S3_REGION, S3_SECRET_KEY, VARIANT_NAME, s3_client)

PARQUET_KEY = "curated/it_vacancies_2024.parquet"

# Условие варианта на SQL, написано независимо от MapReduce. Навыки — уникальные непустые
# значения key_skills без пробелов по краям, в нижнем регистре. Вакансия входит в вариант,
# если роль «Программист, разработчик» и python есть в навыках или в названии.
VARIANT_SQL = """
CREATE OR REPLACE TEMP TABLE variant AS
WITH norm AS (
    SELECT
        vacancy_id,
        role,
        name,
        list_sort(list_distinct(list_filter(
            list_transform(string_split(coalesce(key_skills, ''), ';'), s -> lower(trim(s))),
            s -> s <> ''
        ))) AS skills
    FROM {source}
)
SELECT vacancy_id, skills
FROM norm
WHERE role = 'Программист, разработчик'
  AND (list_contains(skills, 'python') OR contains(lower(coalesce(name, '')), 'python'));
"""

# Пары «вакансия — навык», из них собираются пары и тройки навыков
EDGES_SQL = """
CREATE OR REPLACE TEMP TABLE edges AS
SELECT vacancy_id, unnest(skills) AS s FROM variant;
"""

# Подсчёт сочетаний: пары и тройки через самосоединение с условием a < b < c
K_SQL = {
    1: """SELECT s AS skill1, count(*) AS count
          FROM edges GROUP BY s""",
    2: """SELECT a.s AS skill1, b.s AS skill2, count(*) AS count
          FROM edges a JOIN edges b ON a.vacancy_id = b.vacancy_id AND a.s < b.s
          GROUP BY ALL""",
    3: """SELECT a.s AS skill1, b.s AS skill2, c.s AS skill3, count(*) AS count
          FROM edges a
          JOIN edges b ON a.vacancy_id = b.vacancy_id AND a.s < b.s
          JOIN edges c ON b.vacancy_id = c.vacancy_id AND b.s < c.s
          GROUP BY ALL""",
}
ORDER = {1: "count DESC, skill1", 2: "count DESC, skill1, skill2",
         3: "count DESC, skill1, skill2, skill3"}
NAMES = {1: "skills", 2: "pairs", 3: "triples"}


def connect():
    """Подключает DuckDB к RustFS через httpfs."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    ep = urlparse(S3_ENDPOINT)
    con.execute(f"""
        CREATE OR REPLACE SECRET rustfs (
            TYPE s3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}',
            ENDPOINT '{ep.netloc}', URL_STYLE 'path', USE_SSL {str(ep.scheme == 'https').lower()},
            REGION '{S3_REGION}'
        );""")
    return con


def source_expr(kind):
    """Возвращает SQL-выражение для чтения исходных данных из бакета (CSV или Parquet)."""
    if kind == "parquet":
        return f"read_parquet('s3://{BUCKET}/{PARQUET_KEY}')"
    return (f"read_csv('s3://{BUCKET}/{RAW_KEY}', header=true, all_varchar=true, delim=',', "
            f"quote='\"', escape='\"', max_line_size=10000000)")


def main():
    """Отбирает вакансии варианта, считает сочетания k=1,2,3 и записывает результат."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["csv", "parquet"], default="csv")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--engine-name", default="duckdb")
    ap.add_argument("--threads", type=int, default=0, help="0 = по умолчанию DuckDB")
    args = ap.parse_args()
    if not args.no_write:                          # вывод сохраняется в results/logs/
        tee_log("duckdb_run" if args.source == "csv" else "duckdb_parquet")

    def log(msg=""):
        print(msg, flush=True)

    t_all = time.perf_counter()
    con = connect()
    if args.threads:
        con.execute(f"SET threads = {args.threads}")
    src = source_expr(args.source)
    log(f"DuckDB {duckdb.__version__} | вариант 2: {VARIANT_NAME}")
    log(f"Источник: {src.split('(')[1].split(',')[0].rstrip(')')}")
    log(f"Потоков DuckDB: {con.execute('SELECT current_setting(%s)' % repr('threads')).fetchone()[0]}")

    t0 = time.perf_counter()
    con.execute(VARIANT_SQL.format(source=src))
    con.execute(EDGES_SQL)
    t_filter = time.perf_counter() - t0
    n_var, n_sk = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE len(skills) > 0) FROM variant").fetchone()
    log(f"\n[фильтр варианта] {t_filter:.2f} с")
    log(f"  вакансий варианта: {n_var:,}; из них с навыками: {n_sk:,}")

    # ожидаемое число сочетаний: сумма C(n, k) по вакансиям
    expected = con.execute("""
        SELECT sum(n), sum(n*(n-1)/2), sum(n*(n-1)*(n-2)/6)
        FROM (SELECT len(skills)::BIGINT AS n FROM variant)""").fetchone()

    stats = {"distinct": {}, "total_occurrences": {}, "seconds": {"filter": round(t_filter, 3)}}
    log("\n[подсчёт]")
    for k in (1, 2, 3):
        t0 = time.perf_counter()
        con.execute(f"CREATE OR REPLACE TEMP TABLE r{k} AS {K_SQL[k]}")
        distinct, total = con.execute(f"SELECT count(*), sum(count) FROM r{k}").fetchone()
        dt = time.perf_counter() - t0
        stats["distinct"][f"k{k}"] = distinct
        stats["total_occurrences"][f"k{k}"] = int(total)
        stats["seconds"][f"k{k}"] = round(dt, 3)
        log(f"  k={k}: уникальных {distinct:>9,} | сумма частот {int(total):>10,} | "
            f"ожидалось сочетаний {int(expected[k - 1]):>10,} | {dt:.2f} с")

    log("\n[топ-3]")
    for k in (1, 2, 3):
        cols = ", ".join(f"skill{j}" for j in range(1, k + 1))
        rows = con.execute(f"SELECT {cols}, count FROM r{k} ORDER BY {ORDER[k]} LIMIT 3").fetchall()
        log(f"  k={k}: " + ", ".join(f"{' + '.join(r[:-1])} ({r[-1]})" for r in rows))

    summary = {"engine": args.engine_name, "variant": VARIANT_NAME, "source": args.source,
               "variant_vacancies": n_var, "vacancies_with_skills": n_sk, **stats}

    if not args.no_write:
        t0 = time.perf_counter()
        log("\n[запись результатов в хранилище: COPY ... TO s3://]")
        out_dir = RESULTS_DIR / args.engine_name
        out_dir.mkdir(parents=True, exist_ok=True)
        for k in (1, 2, 3):
            key = f"{RESULTS_PREFIX}/{args.engine_name}/k{k}_{NAMES[k]}.csv"
            con.execute(f"COPY (SELECT * FROM r{k} ORDER BY {ORDER[k]}) "
                        f"TO 's3://{BUCKET}/{key}' (HEADER, DELIMITER ',')")
            con.execute(f"COPY (SELECT * FROM r{k} ORDER BY {ORDER[k]}) "
                        f"TO '{(out_dir / f'k{k}_{NAMES[k]}.csv').as_posix()}' (HEADER, DELIMITER ',')")
            log(f"  s3://{BUCKET}/{key}")
        summary["seconds"]["write"] = round(time.perf_counter() - t0, 3)
        summary["seconds"]["total"] = round(time.perf_counter() - t_all, 3)
        js = json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8")
        (out_dir / "summary.json").write_bytes(js)
        s3_client().put_object(Bucket=BUCKET, Key=f"{RESULTS_PREFIX}/{args.engine_name}/summary.json",
                               Body=js, ContentType="application/json")
        log(f"  s3://{BUCKET}/{RESULTS_PREFIX}/{args.engine_name}/summary.json")
    log(f"\nИтого: {time.perf_counter() - t_all:.2f} с")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
