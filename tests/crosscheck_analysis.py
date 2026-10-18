"""Независимая перепроверка таблиц этапов 5 и 6 на чистом Python, без DuckDB.

Правила те же (условие варианта, зарплата, клоны), реализация своя. Результат построчно
сверяется с таблицами в бакете.

Запуск: python tests/crosscheck_analysis.py
"""
import csv
import io
import statistics as st
import sys
from collections import Counter
from itertools import combinations
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from config import BUCKET, RAW_KEY, RESULTS_PREFIX, s3_client, tee_log  # после добавления src в путь

REGIONS = ["Москва", "Санкт-Петербург", "Екатеринбург"]
EXP = {"Нет опыта": "нет опыта", "От 1 года до 3 лет": "опыт 1–3 года", "От 3 до 6 лет": "опыт 3–6 лет"}


def load_variant(s3):
    """Читает исходный CSV из бакета и отбирает вакансии варианта."""
    csv.field_size_limit(1 << 30)
    body = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)["Body"]
    rows = []
    for r in csv.DictReader(io.TextIOWrapper(body, encoding="utf-8", newline="")):
        sk = tuple(sorted({s.strip().lower() for s in r["key_skills"].split(";") if s.strip()}))
        if r["role"] == "Программист, разработчик" and ("python" in sk or "python" in r["name"].lower()):
            r["sk"] = sk
            rows.append(r)
    return rows


def with_salary(rows):
    """Оставляет вакансии с корректной зарплатой и пересчитывает её на руки."""
    out = []
    for r in rows:
        f, t = r["salary_from"], r["salary_to"]
        if r["currency"] != "RUR" or not (f or t):
            continue
        raw = (float(f) + float(t)) / 2 if f and t else float(f or t)
        sal = raw * 0.87 if r["gross"] == "true" else raw
        if 20_000 <= sal <= 1_000_000:
            r["sal"] = sal
            out.append(r)
    return out


def dedup(rows, by_area):
    """Убирает клоны вакансий по всей стране или внутри города."""
    seen, out = set(), []
    for r in sorted(rows, key=lambda r: int(r["vacancy_id"])):
        key = (r["employer_id"], r["name"], r["salary_from"], r["salary_to"], r["gross"], r["sk"])
        key += (r["area"],) if by_area else ()
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def remote(s3, name):
    """Скачивает таблицу анализа из бакета."""
    body = s3.get_object(Bucket=BUCKET, Key=f"{RESULTS_PREFIX}/analysis/{name}.csv")["Body"].read()
    return list(csv.DictReader(io.StringIO(body.decode("utf-8"))))


class Check:
    """Считает сверенные строки и расхождения."""
    def __init__(self):
        self.total = self.bad = 0

    def table(self, title, expected, actual, key):
        """Сверяет ожидаемую таблицу с таблицей из бакета по ключу и всем полям."""
        act = {key(r): r for r in actual}
        bad = []
        for e in expected:
            a = act.get(key(e))
            if a is None or any(not same(e[c], a[c]) for c in e):
                bad.append((key(e), e, a))
        extra = len(act) - len({key(e) for e in expected})
        self.total += len(expected)
        self.bad += len(bad) + max(extra, 0)
        print(f"  {title:<44} строк {len(expected):>4}: расхождений {len(bad) + max(extra, 0)}")
        for k, e, a in bad[:3]:
            print(f"      {k}: ожидалось {e}, в хранилище {a}")


def same(x, y):
    """Сравнивает значения: числа с допуском, остальное как строки."""
    try:
        return abs(float(x) - float(y)) <= 1e-3 * max(1.0, abs(float(x)))
    except (TypeError, ValueError):
        return str(x) == str(y)


def main():
    """Пересчитывает таблицы этапов 5 и 6 и сверяет их с бакетом."""
    s3 = s3_client()
    variant = load_variant(s3)
    sal_all = with_salary(variant)
    sal, sal_reg = dedup(sal_all, False), dedup(sal_all, True)
    freq = Counter(s for r in variant for s in r["sk"])
    print(f"Независимый пересчёт: вакансий варианта {len(variant)}, с з/п {len(sal_all)}, "
          f"без клонов {len(sal)}, без клонов в городе {len(sal_reg)}\n")
    chk = Check()

    # Этап 5
    exp_rows = [{"experience": e, "vacancies": len(xs), "median_salary": round(st.median(xs))}
                for e in ["Нет опыта", "От 1 года до 3 лет", "От 3 до 6 лет", "Более 6 лет"]
                for xs in [[r["sal"] for r in sal if r["experience"] == e]]]
    chk.table("salary_by_experience", exp_rows, remote(s3, "salary_by_experience"), lambda r: r["experience"])

    def by_freq(skills):
        """Сортирует навыки по убыванию частоты."""
        return sorted(skills, key=lambda s: (-freq[s], s))

    def med_with(rows, need):
        """Число и медиана зарплат вакансий, где есть все навыки need."""
        xs = [r["sal"] for r in rows if need <= set(r["sk"])]
        return len(xs), (st.median(xs) if xs else None)

    pairs = Counter(p for r in sal for p in combinations(r["sk"], 2))
    pair_rows = []
    for (a, b), n in pairs.items():
        if n < 30:
            continue
        base, added = by_freq([a, b])
        bn, bm = med_with(sal, {base})
        _, pm = med_with(sal, {a, b})
        pair_rows.append({"base": base, "added": added, "base_freq": freq[base], "base_vacancies": bn,
                          "base_median": round(bm), "pair_vacancies": n, "pair_median": round(pm),
                          "delta": round(pm - bm)})
    chk.table("pair_uplift (все пары n ≥ 30)", pair_rows, remote(s3, "pair_uplift"),
              lambda r: (r["base"], r["added"]))

    triples = Counter(t for r in sal for t in combinations(r["sk"], 3))
    tri_rows = []
    for key, n in triples.items():
        if n < 20:
            continue
        x1, x2, x3 = by_freq(key)
        bn, bm = med_with(sal, {x1, x2})
        _, tm = med_with(sal, set(key))
        tri_rows.append({"base_pair": f"{x1} + {x2}", "added": x3, "base_vacancies": bn,
                         "base_median": round(bm), "triple_vacancies": n, "triple_median": round(tm),
                         "delta": round(tm - bm)})
    chk.table("triple_uplift (все тройки n ≥ 20)", tri_rows, remote(s3, "triple_uplift"),
              lambda r: (r["base_pair"], r["added"]))

    # Этап 6
    km = []
    for reg in REGIONS:
        rv = [r for r in variant if r["area"] == reg]
        rs = [r["sal"] for r in sal_reg if r["area"] == reg]
        q1, _, q3 = st.quantiles(rs, n=4, method="inclusive")   # так же, как quantile_cont в DuckDB
        nsk = [len(r["sk"]) for r in rv if r["sk"]]
        vals = {
            "Вакансий варианта": len(rv),
            "Доля от всего сегмента": len(rv) / len(variant),
            "Вакансий с навыками": len(nsk),
            "Медиана числа навыков": st.median(nsk),
            "С з/п, без клонов в городе": len(rs),
            "Доля вакансий с з/п": sum(1 for r in sal_all if r["area"] == reg) / len(rv),
            "Медиана з/п на руки, ₽": st.median(rs),
            "Q1 з/п, ₽": q1,
            "Q3 з/п, ₽": q3,
        }
        for e, label in EXP.items():
            vals[f"Медиана: {label}, ₽"] = st.median(
                [r["sal"] for r in sal_reg if r["area"] == reg and r["experience"] == e])
        vals["Доля удалённой работы"] = sum(r["schedule"] == "Удаленная работа" for r in rv) / len(rv)
        vals["Доля «нет опыта»"] = sum(r["experience"] == "Нет опыта" for r in rv) / len(rv)
        vals["Доля «более 6 лет»"] = sum(r["experience"] == "Более 6 лет" for r in rv) / len(rv)
        km.append((reg, vals))
    exp_km = [{"metric": m, **{reg: v[m] for reg, v in km}} for m in km[0][1]]
    chk.table("regions_key_metrics", exp_km, remote(s3, "regions_key_metrics"), lambda r: r["metric"])

    demand = []
    for reg in REGIONS:
        rv = [r for r in variant if r["area"] == reg]
        n_sk = sum(1 for r in rv if r["sk"])
        for k, limit in ((1, 10), (2, 5), (3, 5)):
            c = Counter(" + ".join(t) for r in rv for t in combinations(r["sk"], k))
            top = sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]
            demand += [{"k": k, "region": reg, "rank": i + 1, "combo": combo, "vacancies": cnt,
                        "share": round(cnt / n_sk, 4)} for i, (combo, cnt) in enumerate(top)]
    chk.table("regions_skill_demand (навыки, пары, тройки)", demand, remote(s3, "regions_skill_demand"),
              lambda r: (str(r["k"]), r["region"], str(r["rank"])))

    lead = [s for s, _ in sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))[:10]]
    sbs = []
    for s in lead:
        for reg in REGIONS:
            xs = [r["sal"] for r in sal_reg if r["area"] == reg and s in r["sk"]]
            sbs.append({"skill": s, "region": reg, "vacancies_with_salary": len(xs),
                        "median_salary": round(st.median(xs)) if len(xs) >= 10 else ""})
    chk.table("regions_salary_by_skill", sbs, remote(s3, "regions_salary_by_skill"),
              lambda r: (r["skill"], r["region"]))

    print(f"\nИТОГ: сверено {chk.total} строк, расхождений {chk.bad} — "
          f"{'таблицы этапов 5–6 ПОДТВЕРЖДЕНЫ' if chk.bad == 0 else 'ЕСТЬ РАСХОЖДЕНИЯ'}")
    sys.exit(1 if chk.bad else 0)


if __name__ == "__main__":
    tee_log("crosscheck_analysis")
    main()
