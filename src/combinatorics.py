"""Объясняет рост числа пар и троек числом сочетаний навыков внутри вакансии.

Вакансия с n навыками даёт C(n,2) пар и C(n,3) троек. Скрипт показывает распределение n
и вклад вакансий с разным числом навыков в общее число сочетаний.

Запуск: python src/combinatorics.py
"""
import sys
from config import tee_log
from math import comb

from duckdb_counts import VARIANT_SQL, connect, source_expr
from variant_data import save_table


def main():
    """Считает распределение числа навыков и вклад групп вакансий в сочетания."""
    con = connect()
    con.execute(VARIANT_SQL.format(source=source_expr("parquet")))
    con.execute("""CREATE TEMP TABLE n AS SELECT len(skills)::BIGINT AS n FROM variant WHERE len(skills) > 0""")
    cnt, mean, med, p90, mx = con.execute(
        "SELECT count(*), avg(n), median(n), quantile_cont(n, 0.9), max(n) FROM n").fetchone()
    print(f"Вакансий с навыками: {cnt:,}; навыков на вакансию: среднее {mean:.2f}, медиана {med:g}, "
          f"90-й перцентиль {p90:g}, максимум {mx}")

    rows = con.execute("""
        SELECT CASE WHEN n <= 3 THEN '1–3' WHEN n <= 6 THEN '4–6' WHEN n <= 10 THEN '7–10'
                    WHEN n <= 15 THEN '11–15' ELSE '16+' END AS b,
               min(n), count(*), sum(n), sum(n*(n-1)//2), sum(n*(n-1)*(n-2)//6)
        FROM n GROUP BY 1 ORDER BY 2""").fetchall()
    tot = [sum(r[i] for r in rows) for i in (2, 3, 4, 5)]
    print("\nВклад групп вакансий в число сочетаний:")
    print(f"  {'навыков':>8} | {'вакансий':>14} | {'навыков (k=1)':>16} | {'пар (k=2)':>16} | {'троек (k=3)':>17}")
    out = []
    for b, _, v, s1, s2, s3 in rows:
        print(f"  {b:>8} | {v:>6,} ({v / tot[0]:5.1%}) | {s1:>7,} ({s1 / tot[1]:5.1%}) | "
              f"{s2:>7,} ({s2 / tot[2]:5.1%}) | {s3:>8,} ({s3 / tot[3]:5.1%})")
        out.append((b, v, s1, s2, s3))
    print(f"  {'итого':>8} | {tot[0]:>14,} | {tot[1]:>16,} | {tot[2]:>16,} | {tot[3]:>17,}")

    print("\nПримеры: вакансия с n навыками даёт C(n,2) пар и C(n,3) троек")
    for n in (3, 5, 7, 10, 15, 20, int(mx)):
        print(f"  n = {n:>2}: пар {comb(n, 2):>4}, троек {comb(n, 3):>5}")

    vocab = con.execute("SELECT count(DISTINCT s) FROM (SELECT unnest(skills) AS s FROM variant)").fetchone()[0]
    obs = [con.execute(q).fetchone()[0] for q in (
        "SELECT count(DISTINCT s) FROM (SELECT unnest(skills) s FROM variant)",
        "SELECT count(*) FROM (SELECT DISTINCT a.s, b.s FROM (SELECT vacancy_id, unnest(skills) s FROM variant) a "
        "JOIN (SELECT vacancy_id, unnest(skills) s FROM variant) b ON a.vacancy_id = b.vacancy_id AND a.s < b.s)",
    )]
    print(f"\nСловарь навыков: {vocab:,}. Теоретически возможно пар C({vocab},2) = {comb(vocab, 2):,}, "
          f"троек C({vocab},3) = {comb(vocab, 3):,}")
    print(f"Наблюдаемых уникальных пар {obs[1]:,} — {obs[1] / comb(vocab, 2):.2%} от возможных")
    save_table("combinatorics_by_skill_count", ["skills_bucket", "vacancies", "k1", "k2", "k3"], out)


if __name__ == "__main__":
    tee_log("combinatorics")
    main()
