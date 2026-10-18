"""Общая подготовка данных для этапов 5 и 6: вакансии варианта и их зарплаты.

Зарплата берётся только в рублях: середина вилки или единственная граница, сумма до вычета
налога пересчитывается на руки (НДФЛ 13 %), значения вне 20 000–1 000 000 отбрасываются.
Клоны — одинаковые вакансии одного работодателя в разных городах — учитываются один раз.
"""
import csv
import io

from config import BUCKET, RESULTS_DIR, RESULTS_PREFIX, s3_client
from duckdb_counts import VARIANT_SQL, connect, source_expr

SAL_MIN, SAL_MAX, NDFL = 20_000, 1_000_000, 0.13

FULL_VARIANT_SQL = VARIANT_SQL.replace("SELECT vacancy_id, skills\nFROM norm",
                                       "SELECT * EXCLUDE (role) FROM norm")


def load(con=None, dedup=True):
    """Создаёт таблицы variant, sal (без клонов по стране) и sal_reg (без клонов в городе)."""
    con = con or connect()
    src = source_expr("csv")
    # то же условие варианта, что на этапе 3, плюс колонки для анализа
    sql = FULL_VARIANT_SQL.replace(
        "        vacancy_id,\n        role,\n        name,",
        "        vacancy_id, role, name, area, experience, schedule, salary_from, salary_to,\n"
        "        currency, gross, employer_id,")
    con.execute(sql.format(source=src))
    clone_key = "employer_id, name, salary_from, salary_to, gross, skills"
    # vacancy_id прочитан как текст, поэтому сортируем по числу
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE sal_all AS
        WITH s AS (
            SELECT *,
                CASE WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                     THEN (salary_from::DOUBLE + salary_to::DOUBLE) / 2
                     ELSE coalesce(salary_from, salary_to)::DOUBLE END AS sal_raw
            FROM variant WHERE currency = 'RUR'
        )
        SELECT *, CASE WHEN gross = 'true' THEN sal_raw * (1 - {NDFL}) ELSE sal_raw END AS salary,
               row_number() OVER (PARTITION BY {clone_key} ORDER BY vacancy_id::BIGINT) AS clone_no,
               row_number() OVER (PARTITION BY {clone_key}, area ORDER BY vacancy_id::BIGINT) AS clone_no_area
        FROM s
        WHERE (CASE WHEN gross = 'true' THEN sal_raw * (1 - {NDFL}) ELSE sal_raw END)
              BETWEEN {SAL_MIN} AND {SAL_MAX};
    """)
    # sal — без клонов по всей стране (этап 5), sal_reg — без клонов внутри города (этап 6)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sal AS
                    SELECT * FROM sal_all {'WHERE clone_no = 1' if dedup else ''}""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sal_reg AS
                    SELECT * FROM sal_all {'WHERE clone_no_area = 1' if dedup else ''}""")
    return con


def load_skill_freq(con, engine="mapreduce"):
    """Загружает частоты навыков из результата этапа 3 в хранилище."""
    body = s3_client().get_object(Bucket=BUCKET, Key=f"{RESULTS_PREFIX}/{engine}/k1_skills.csv")
    rows = list(csv.reader(io.StringIO(body["Body"].read().decode("utf-8"))))[1:]
    con.execute("CREATE OR REPLACE TEMP TABLE freq (s VARCHAR, f BIGINT)")
    con.executemany("INSERT INTO freq VALUES (?, ?)", [(r[0], int(r[1])) for r in rows])
    return len(rows)


def save_table(name, header, rows, log=print):
    """Сохраняет таблицу анализа в бакет и в локальную папку."""
    out = RESULTS_DIR / "analysis"
    out.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    body = buf.getvalue().encode("utf-8")
    (out / f"{name}.csv").write_bytes(body)
    s3_client().put_object(Bucket=BUCKET, Key=f"{RESULTS_PREFIX}/analysis/{name}.csv", Body=body,
                           ContentType="text/csv")
    log(f"  -> s3://{BUCKET}/{RESULTS_PREFIX}/analysis/{name}.csv")
