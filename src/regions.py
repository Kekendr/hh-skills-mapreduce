"""Этап 6. Сравнение трёх регионов: Москва, Санкт-Петербург, Екатеринбург.

Выбраны три крупнейших по числу вакансий варианта. Считаются ключевые показатели и зарплаты,
востребованность навыков, пар и троек, отличительные навыки и зарплаты по ведущим навыкам.
Клоны вакансий убираются только внутри города: копии в разных городах — разные вакансии.

Запуск: python src/regions.py
"""
import sys
import time

from config import tee_log
from variant_data import load, save_table

REGIONS = ["Москва", "Санкт-Петербург", "Екатеринбург"]
REG_SQL = ", ".join(f"'{r}'" for r in REGIONS)
TOP_SKILLS, TOP_COMBOS, MIN_LIFT_N, MIN_SAL_N = 10, 5, 15, 10


def fmt(x):
    return "—" if x is None else f"{x:,.0f}".replace(",", " ")


def pct(x):
    return "—" if x is None else f"{x:.1%}"


def main():
    """Строит четыре таблицы сравнения регионов и сохраняет их в бакет."""
    t0 = time.perf_counter()
    con = load()
    print(f"Этап 6. Регионы: {', '.join(REGIONS)}")

    # Таблица 1: ключевые показатели
    metrics = [
        ("Вакансий варианта", "SELECT count(*) FROM variant WHERE area = ?", "int"),
        ("Доля от всего сегмента", "SELECT count(*) / (SELECT count(*) FROM variant) FROM variant WHERE area = ?", "pct"),
        ("Вакансий с навыками", "SELECT count(*) FILTER (WHERE len(skills) > 0) FROM variant WHERE area = ?", "int"),
        ("Медиана числа навыков", "SELECT median(len(skills)) FILTER (WHERE len(skills) > 0) FROM variant WHERE area = ?", "num"),
        ("С з/п, без клонов в городе", "SELECT count(*) FROM sal_reg WHERE area = ?", "int"),
        ("Доля вакансий с з/п", "SELECT (SELECT count(*) FROM sal_all WHERE area = $1) / count(*) FROM variant WHERE area = $1", "pct"),
        ("Медиана з/п на руки, ₽", "SELECT median(salary) FROM sal_reg WHERE area = ?", "rub"),
        ("Q1 з/п, ₽", "SELECT quantile_cont(salary, 0.25) FROM sal_reg WHERE area = ?", "rub"),
        ("Q3 з/п, ₽", "SELECT quantile_cont(salary, 0.75) FROM sal_reg WHERE area = ?", "rub"),
        ("Медиана: нет опыта, ₽", "SELECT median(salary) FROM sal_reg WHERE area = ? AND experience = 'Нет опыта'", "rub"),
        ("Медиана: опыт 1–3 года, ₽", "SELECT median(salary) FROM sal_reg WHERE area = ? AND experience = 'От 1 года до 3 лет'", "rub"),
        ("Медиана: опыт 3–6 лет, ₽", "SELECT median(salary) FROM sal_reg WHERE area = ? AND experience = 'От 3 до 6 лет'", "rub"),
        ("Доля удалённой работы", "SELECT avg((schedule = 'Удаленная работа')::INT) FROM variant WHERE area = ?", "pct"),
        ("Доля «нет опыта»", "SELECT avg((experience = 'Нет опыта')::INT) FROM variant WHERE area = ?", "pct"),
        ("Доля «более 6 лет»", "SELECT avg((experience = 'Более 6 лет')::INT) FROM variant WHERE area = ?", "pct"),
    ]
    render = {"int": lambda v: fmt(v), "pct": pct, "rub": fmt, "num": lambda v: f"{v:g}"}
    table1 = []
    for title, sql, kind in metrics:
        vals = [con.execute(sql, [r]).fetchone()[0] for r in REGIONS]
        table1.append((title, *vals, kind))
    seg_med = con.execute("SELECT median(salary) FROM sal").fetchone()[0]
    print("\nТаблица 1. Ключевые показатели")
    print(f"  {'показатель':<30} | " + " | ".join(f"{r:>16}" for r in REGIONS))
    for title, *vals, kind in table1:
        print(f"  {title:<30} | " + " | ".join(f"{render[kind](v):>16}" for v in vals))
    print(f"  (медиана по всему сегменту: {fmt(seg_med)} ₽)")
    save_table("regions_key_metrics", ["metric", *REGIONS],
               [(t, *[None if v is None else round(v, 4) for v in vals]) for t, *vals, _ in table1])

    # Таблица 2: навыки, пары и тройки
    con.execute(f"""CREATE OR REPLACE TEMP TABLE re AS
                    SELECT area, vacancy_id, unnest(skills) AS s FROM variant WHERE area IN ({REG_SQL})""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rn AS
                    SELECT area, count(*) FILTER (WHERE len(skills) > 0) AS n FROM variant
                    WHERE area IN ({REG_SQL}) GROUP BY area""")
    combo_sql = {
        1: "SELECT area, s AS combo, count(*) AS c FROM re GROUP BY ALL",
        2: """SELECT a.area, a.s || ' + ' || b.s AS combo, count(*) AS c FROM re a
              JOIN re b ON a.vacancy_id = b.vacancy_id AND a.s < b.s GROUP BY ALL""",
        3: """SELECT a.area, a.s || ' + ' || b.s || ' + ' || c.s AS combo, count(*) AS c FROM re a
              JOIN re b ON a.vacancy_id = b.vacancy_id AND a.s < b.s
              JOIN re c ON b.vacancy_id = c.vacancy_id AND b.s < c.s GROUP BY ALL""",
    }
    names = {1: "навыки", 2: "пары", 3: "тройки"}
    print("\nТаблица 2. Востребованность навыков (доля вакансий региона с указанными навыками)")
    t2rows = []
    for k in (1, 2, 3):
        limit = TOP_SKILLS if k == 1 else TOP_COMBOS
        res = con.execute(f"""
            WITH x AS ({combo_sql[k]})
            SELECT x.area, combo, c, c / rn.n AS share,
                   row_number() OVER (PARTITION BY x.area ORDER BY c DESC, combo) AS rk
            FROM x JOIN rn USING (area) QUALIFY rk <= {limit} ORDER BY x.area, rk""").fetchall()
        by_reg = {r: [x for x in res if x[0] == r] for r in REGIONS}
        print(f"\n  Топ-{limit}, {names[k]}:")
        print(f"  {'#':>2} | " + " | ".join(f"{r:<53}" for r in REGIONS))
        for i in range(limit):
            cells = []
            for r in REGIONS:
                if i < len(by_reg[r]):
                    _, combo, c, sh, _ = by_reg[r][i]
                    cells.append(f"{combo[:40]:<40} {c:>4} {sh:>6.1%}")
                else:
                    cells.append(" " * 53)
            print(f"  {i + 1:>2} | " + " | ".join(f"{c:<53}" for c in cells))
        t2rows += [(k, a, rk, combo, c, round(sh, 4)) for a, combo, c, sh, rk in res]
    save_table("regions_skill_demand", ["k", "region", "rank", "combo", "vacancies", "share"], t2rows)

    # Таблица 3: отличительные навыки
    lift = con.execute(f"""
        WITH seg AS (SELECT s, count(*) / (SELECT count(*) FILTER (WHERE len(skills) > 0) FROM variant) AS sh
                     FROM (SELECT unnest(skills) AS s FROM variant) GROUP BY s),
             reg AS (SELECT area, s, count(*) AS c FROM re GROUP BY ALL)
        SELECT reg.area, reg.s, reg.c, reg.c / rn.n AS sh_reg, seg.sh AS sh_seg,
               (reg.c / rn.n) / seg.sh AS lift,
               row_number() OVER (PARTITION BY reg.area ORDER BY (reg.c / rn.n) / seg.sh DESC, reg.s) AS rk
        FROM reg JOIN rn USING (area) JOIN seg USING (s)
        WHERE reg.c >= {MIN_LIFT_N}
        QUALIFY rk <= 6 ORDER BY reg.area, rk""").fetchall()
    print(f"\nТаблица 3. Отличительные навыки региона (≥ {MIN_LIFT_N} вакансий; во сколько раз доля "
          f"выше, чем по сегменту)")
    for r in REGIONS:
        items = [x for x in lift if x[0] == r]
        print(f"  {r}: " + "; ".join(f"{s} ×{lf:.1f} ({shr:.1%} против {shs:.1%})"
                                    for _, s, c, shr, shs, lf, _ in items))
    save_table("regions_distinctive_skills", ["region", "skill", "vacancies", "share_region",
                                              "share_segment", "lift"],
               [(a, s, c, round(shr, 4), round(shs, 4), round(lf, 3)) for a, s, c, shr, shs, lf, _ in lift])

    # Таблица 4: зарплата по ведущим навыкам
    lead = [r[0] for r in con.execute(
        "SELECT s FROM (SELECT unnest(skills) AS s FROM variant) GROUP BY s ORDER BY count(*) DESC, s LIMIT 10"
    ).fetchall()]
    print(f"\nТаблица 4. Медиана з/п на руки по ведущим навыкам сегмента, ₽ (n — вакансий с з/п без клонов в городе;"
          f" «—» при n < {MIN_SAL_N})")
    print(f"  {'навык':<17} | " + " | ".join(f"{r:>22}" for r in REGIONS))
    t4rows = []
    for s in lead:
        cells = []
        for r in REGIONS:
            n, m = con.execute("SELECT count(*), median(salary) FROM sal_reg WHERE area = ? AND list_contains(skills, ?)",
                               [r, s]).fetchone()
            cells.append(f"{fmt(m if n >= MIN_SAL_N else None):>12} (n={n:>3})")
            t4rows.append((s, r, n, None if n < MIN_SAL_N else round(m)))
        print(f"  {s:<17} | " + " | ".join(f"{c:>22}" for c in cells))
    save_table("regions_salary_by_skill", ["skill", "region", "vacancies_with_salary", "median_salary"], t4rows)
    print(f"\nИтого: {time.perf_counter() - t0:.2f} с")


if __name__ == "__main__":
    tee_log("regions")
    main()
