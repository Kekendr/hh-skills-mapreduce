"""Этап 2. Сохраняет вакансии варианта отдельным объектом в зоне segment/ бакета.

MapReduce и DuckDB отбирают вариант из полного файла сами, выгрузка — дополнительная копия
сегмента. Скрипт проверяет, что по ней получаются те же контрольные числа, что на этапе 3.

Запуск: python src/export_segment.py
"""
import time

from config import BUCKET, tee_log
from duckdb_counts import VARIANT_SQL, connect, source_expr

SEGMENT_KEY = "segment/variant2_python_developers.csv"


def main():
    """Выгружает сегмент и сверяет его контрольные числа с этапом 3."""
    con = connect()
    t0 = time.perf_counter()
    raw = source_expr("csv")
    con.execute(VARIANT_SQL.format(source=raw))
    con.execute(f"""
        COPY (SELECT src.* FROM {raw} AS src
              JOIN variant USING (vacancy_id)
              ORDER BY src.vacancy_id::BIGINT)
        TO 's3://{BUCKET}/{SEGMENT_KEY}' (HEADER, DELIMITER ',')""")
    print(f"Сегмент «Программист, разработчик на Python» записан: s3://{BUCKET}/{SEGMENT_KEY} "
          f"за {time.perf_counter() - t0:.1f} с")

    seg = (f"read_csv('s3://{BUCKET}/{SEGMENT_KEY}', header=true, all_varchar=true, delim=',', "
           f"quote='\"', escape='\"')")
    rows, ids = con.execute(f"SELECT count(*), count(DISTINCT vacancy_id) FROM {seg}").fetchone()
    ncols = len(con.execute(f"DESCRIBE SELECT * FROM {seg}").fetchall())
    print(f"Проверка выгрузки: строк {rows:,}, уникальных vacancy_id {ids:,}, колонок {ncols}")

    con.execute(VARIANT_SQL.format(source=seg).replace("TEMP TABLE variant", "TEMP TABLE variant_seg"))
    n_sk, k1, k2, k3 = con.execute("""
        SELECT count(*) FILTER (WHERE len(skills) > 0), sum(len(skills)),
               sum(len(skills) * (len(skills) - 1) // 2),
               sum(len(skills) * (len(skills) - 1) * (len(skills) - 2) // 6)
        FROM variant_seg""").fetchone()
    print(f"Повторный отбор варианта внутри выгрузки: {con.execute('SELECT count(*) FROM variant_seg').fetchone()[0]:,} "
          f"вакансий (условие идемпотентно)")
    print(f"Контрольные числа по выгрузке: с навыками {n_sk:,}; сумма частот k1 {int(k1):,}, "
          f"k2 {int(k2):,}, k3 {int(k3):,}")
    ok = (rows, n_sk, int(k1), int(k2), int(k3)) == (7545, 6637, 48857, 223836, 896960)
    print(f"Совпадают с этапом 3 (7 545 / 6 637 / 48 857 / 223 836 / 896 960): {ok}")


if __name__ == "__main__":
    tee_log("export_segment")
    main()
