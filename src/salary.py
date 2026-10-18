"""Этап 5. Какие пары и тройки навыков повышают и понижают медианную зарплату.

Для пары базой служит более частый навык, для тройки — пара двух более частых навыков.
Прибавка — разница медиан вакансий с сочетанием и с базой. Учитываются пары
с 30 и более вакансиями с зарплатой, тройки — с 20 и более.

Запуск: python src/salary.py
"""
import sys
import time

from config import tee_log
from variant_data import SAL_MAX, SAL_MIN, load, load_skill_freq, save_table

MIN_PAIR, MIN_TRIPLE, TOP = 30, 20, 10


def fmt(x):
    return f"{x:,.0f}".replace(",", " ")


def print_table(title, header, rows, widths):
    """Печатает таблицу с выравниванием колонок."""
    print(f"\n{title}")
    print("  " + " | ".join(h.rjust(w) if i else h.ljust(w) for i, (h, w) in enumerate(zip(header, widths))))
    for r in rows:
        print("  " + " | ".join(str(c).rjust(w) if i else str(c).ljust(w)
                                for i, (c, w) in enumerate(zip(r, widths))))


def main():
    """Считает медианы по опыту, числу навыков, квартилям и прибавки от пар и троек."""
    t0 = time.perf_counter()
    con = load()
    n_freq = load_skill_freq(con)
    n_var, n_sal = con.execute("SELECT (SELECT count(*) FROM variant), count(*) FROM sal").fetchone()
    q1, med, q3 = con.execute("SELECT quantile_cont(salary, [0.25, 0.5, 0.75]) FROM sal").fetchone()[0]
    print(f"Этап 5. Зарплаты | вакансий варианта {n_var:,}, с зарплатой в ₽ в диапазоне "
          f"[{fmt(SAL_MIN)}; {fmt(SAL_MAX)}] на руки: {n_sal:,} ({n_sal / n_var:.1%})")
    print(f"Частоты навыков загружены из результата этапа 3: {n_freq:,} навыков")
    print(f"Медиана на руки: {fmt(med)} ₽ (Q1 {fmt(q1)} — Q3 {fmt(q3)})")

    # Клоны вакансий
    n_all, n_groups, n_cloned = con.execute("""
        SELECT count(*), count(*) FILTER (WHERE clone_no = 1),
               count(DISTINCT (employer_id, name, salary_from, salary_to, gross, skills))
                   FILTER (WHERE clone_no = 2)
        FROM sal_all""").fetchone()
    print(f"\nКлоны вакансий (один работодатель, то же название, вилка и навыки в разных городах):")
    print(f"  вакансий с зарплатой до удаления клонов: {n_all:,}; после: {n_groups:,}; "
          f"удалено копий: {n_all - n_groups:,}; групп-клонов: {n_cloned:,}")
    big = con.execute("""
        SELECT employer_id, any_value(name), count(*), count(DISTINCT area), any_value(salary)
        FROM sal_all GROUP BY employer_id, name, salary_from, salary_to, gross, skills
        HAVING count(*) >= 5 ORDER BY 3 DESC LIMIT 5""").fetchall()
    print_table("  Крупнейшие группы клонов:", ["работодатель", "вакансия", "копий", "городов", "з/п, ₽"],
                [(f"id {e}", n[:38], c, a, fmt(s)) for e, n, c, a, s in big], [12, 38, 6, 7, 9])
    effect = []
    for label, cond in [("python + clickhouse", "['python', 'clickhouse']"),
                        ("python + airflow", "['python', 'airflow']"),
                        ("sql + c++ + c#", "['sql', 'c++', 'c#']")]:
        b = con.execute(f"SELECT count(*), median(salary) FROM sal_all WHERE list_has_all(skills, {cond})").fetchone()
        a = con.execute(f"SELECT count(*), median(salary) FROM sal WHERE list_has_all(skills, {cond})").fetchone()
        effect.append((label, b[0], fmt(b[1]), a[0], fmt(a[1])))
    print_table("  Влияние клонов на медиану:", ["сочетание", "вак. с клонами", "медиана",
                                                  "вак. без клонов", "медиана"],
                effect, [20, 14, 9, 15, 9])

    # Зарплата по опыту и числу навыков
    exp = con.execute("""
        SELECT experience, count(*), median(salary) FROM sal GROUP BY 1
        ORDER BY CASE experience WHEN 'Нет опыта' THEN 0 WHEN 'От 1 года до 3 лет' THEN 1
                 WHEN 'От 3 до 6 лет' THEN 2 ELSE 3 END""").fetchall()
    print_table("Медиана по требуемому опыту:", ["опыт", "вакансий", "медиана, ₽"],
                [(e, n, fmt(m)) for e, n, m in exp], [20, 8, 12])
    save_table("salary_by_experience", ["experience", "vacancies", "median_salary"],
               [(e, n, round(m)) for e, n, m in exp])

    nsk = con.execute("""
        SELECT CASE WHEN len(skills) = 0 THEN '0' WHEN len(skills) <= 3 THEN '1–3'
                    WHEN len(skills) <= 6 THEN '4–6' WHEN len(skills) <= 10 THEN '7–10'
                    ELSE '11+' END AS b, count(*), median(salary), min(len(skills))
        FROM sal GROUP BY 1 ORDER BY 4""").fetchall()
    print_table("Медиана по числу навыков в вакансии:", ["навыков", "вакансий", "медиана, ₽"],
                [(b, n, fmt(m)) for b, n, m, _ in nsk], [20, 8, 12])
    save_table("salary_by_skill_count", ["skills_bucket", "vacancies", "median_salary"],
               [(b, n, round(m)) for b, n, m, _ in nsk])

    # Навыки верхнего и нижнего квартиля
    quart = con.execute(f"""
        WITH q AS (SELECT *, CASE WHEN salary >= {q3} THEN 'top' WHEN salary <= {q1} THEN 'bottom' END AS g
                   FROM sal WHERE len(skills) > 0),
        e AS (SELECT g, unnest(skills) AS s FROM q WHERE g IS NOT NULL),
        tot AS (SELECT g, count(*) AS n FROM q WHERE g IS NOT NULL GROUP BY g)
        SELECT s,
               count(*) FILTER (WHERE e.g = 'top')    / any_value(tt.n) AS share_top,
               count(*) FILTER (WHERE e.g = 'bottom') / any_value(tb.n) AS share_bottom
        FROM e, (SELECT n FROM tot WHERE g = 'top') tt, (SELECT n FROM tot WHERE g = 'bottom') tb
        GROUP BY s HAVING count(*) >= 40
        ORDER BY share_top - share_bottom DESC""").fetchall()
    rows = [(s, f"{t:.1%}", f"{b:.1%}", f"{(t - b) * 100:+.1f} п.п.") for s, t, b in quart]
    print_table("Навыки верхнего квартиля зарплат (чаще, чем в нижнем):",
                ["навык", "в верхн. 25%", "в нижн. 25%", "разница"], rows[:TOP], [22, 12, 12, 12])
    print_table("Навыки нижнего квартиля зарплат (чаще, чем в верхнем):",
                ["навык", "в верхн. 25%", "в нижн. 25%", "разница"], rows[::-1][:TOP], [22, 12, 12, 12])
    save_table("skills_top_vs_bottom_quartile", ["skill", "share_top25", "share_bottom25"],
               [(s, round(t, 4), round(b, 4)) for s, t, b in quart])

    # Медианы по навыкам, парам и тройкам
    con.execute("CREATE OR REPLACE TEMP TABLE se AS SELECT vacancy_id, salary, unnest(skills) AS s FROM sal")
    m1 = {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT s, count(*), median(salary) FROM se GROUP BY s").fetchall()}
    m2 = {(r[0], r[1]): (r[2], r[3]) for r in con.execute("""
        SELECT a.s, b.s, count(*), median(a.salary) FROM se a
        JOIN se b ON a.vacancy_id = b.vacancy_id AND a.s < b.s GROUP BY ALL""").fetchall()}
    m3 = {(r[0], r[1], r[2]): (r[3], r[4]) for r in con.execute(f"""
        SELECT a.s, b.s, c.s, count(*), median(a.salary) FROM se a
        JOIN se b ON a.vacancy_id = b.vacancy_id AND a.s < b.s
        JOIN se c ON b.vacancy_id = c.vacancy_id AND b.s < c.s
        GROUP BY ALL HAVING count(*) >= {MIN_TRIPLE}""").fetchall()}
    freq = dict(con.execute("SELECT s, f FROM freq").fetchall())

    def by_freq(skills):            # по убыванию частоты, при равенстве — по алфавиту
        return sorted(skills, key=lambda s: (-freq.get(s, 0), s))

    pairs = []
    for (a, b), (n, pm) in m2.items():
        if n < MIN_PAIR:
            continue
        base, added = by_freq([a, b])
        bn, bm = m1[base]
        pairs.append((base, added, freq[base], bn, bm, n, pm, pm - bm, (pm - bm) / bm))
    triples = []
    for key, (n, tm) in m3.items():
        x1, x2, x3 = by_freq(key)
        bn, bm = m2[tuple(sorted((x1, x2)))]
        triples.append((f"{x1} + {x2}", x3, bn, bm, n, tm, tm - bm, (tm - bm) / bm))

    hp = ["база (частый навык)", "+ навык", "частота базы", "вак. базы", "медиана базы",
          "вак. пары", "медиана пары", "Δ, ₽", "Δ, %"]
    def prow(p):
        return (p[0], p[1], p[2], p[3], fmt(p[4]), p[5], fmt(p[6]), f"{p[7]:+,.0f}".replace(",", " "),
                f"{p[8]:+.1%}")
    w = [20, 18, 8, 8, 12, 8, 12, 10, 8]
    pairs.sort(key=lambda p: p[7], reverse=True)
    print(f"\nПар с ≥ {MIN_PAIR} вакансиями с зарплатой: {len(pairs)}")
    print_table("ПАРЫ, сильнее всего ПОВЫШАЮЩИЕ медиану:", hp, [prow(p) for p in pairs[:TOP]], w)
    print_table("ПАРЫ, сильнее всего ПОНИЖАЮЩИЕ медиану:", hp, [prow(p) for p in pairs[::-1][:TOP]], w)
    save_table("pair_uplift", ["base", "added", "base_freq", "base_vacancies", "base_median",
                               "pair_vacancies", "pair_median", "delta", "delta_pct"],
               [(p[0], p[1], p[2], p[3], round(p[4]), p[5], round(p[6]), round(p[7]), round(p[8], 4))
                for p in pairs])

    ht = ["база (частая пара)", "+ навык", "вак. базы", "медиана базы", "вак. тройки",
          "медиана тройки", "Δ, ₽", "Δ, %"]
    def trow(t):
        return (t[0], t[1], t[2], fmt(t[3]), t[4], fmt(t[5]), f"{t[6]:+,.0f}".replace(",", " "),
                f"{t[7]:+.1%}")
    wt = [28, 18, 8, 12, 8, 12, 10, 8]
    triples.sort(key=lambda t: t[6], reverse=True)
    print(f"\nТроек с ≥ {MIN_TRIPLE} вакансиями с зарплатой: {len(triples)}")
    print_table("ТРОЙКИ, сильнее всего ПОВЫШАЮЩИЕ медиану:", ht, [trow(t) for t in triples[:TOP]], wt)
    print_table("ТРОЙКИ, сильнее всего ПОНИЖАЮЩИЕ медиану:", ht, [trow(t) for t in triples[::-1][:TOP]], wt)
    save_table("triple_uplift", ["base_pair", "added", "base_vacancies", "base_median",
                                 "triple_vacancies", "triple_median", "delta", "delta_pct"],
               [(t[0], t[1], t[2], round(t[3]), t[4], round(t[5]), round(t[6]), round(t[7], 4))
                for t in triples])
    print(f"\nИтого: {time.perf_counter() - t0:.2f} с")


if __name__ == "__main__":
    tee_log("salary")
    main()
