"""Находка о качестве данных: навыки-двойники с кириллицей вместо латиницы.

Например, «с#» набран с русской «с» и считается отдельным от «c#» навыком. Скрипт находит
такие группы и оценивает масштаб. В основном подсчёте двойники не склеиваются, чтобы правило
нормализации было одинаковым и простым в MapReduce и DuckDB.

Запуск: python src/data_quality.py
"""
import csv
import io

from config import BUCKET, RESULTS_PREFIX, s3_client, tee_log

HOMOGLYPHS = str.maketrans("асеорхукмтвн", "aceopxykmtbh")  # похожие кириллические и латинские буквы


def main():
    """Находит группы навыков-двойников и печатает их частоты."""
    body = s3_client().get_object(Bucket=BUCKET, Key=f"{RESULTS_PREFIX}/mapreduce/k1_skills.csv")["Body"]
    freq = {r[0]: int(r[1]) for r in list(csv.reader(io.StringIO(body.read().decode("utf-8"))))[1:]}
    groups = {}
    for skill, cnt in freq.items():
        has_latin = any("a" <= ch <= "z" for ch in skill)
        key = skill.translate(HOMOGLYPHS) if has_latin or len(skill) <= 4 else skill
        groups.setdefault(key, []).append((skill, cnt))
    mixed = sorted((v for v in groups.values() if len(v) > 1), key=lambda v: -sum(c for _, c in v))

    def show(s):
        """Помечает кириллические буквы в названии навыка."""
        marks = "".join("^" if "а" <= ch <= "я" else " " for ch in s)
        return f"{s!r}" + (f" (кириллица: {marks.rstrip()})" if marks.strip() else "")

    print(f"Навыков в результате этапа 3: {len(freq):,}; упоминаний: {sum(freq.values()):,}")
    print(f"Групп навыков, различающихся только кириллическими «двойниками» латинских букв: {len(mixed)}\n")
    for v in mixed[:10]:
        v = sorted(v, key=lambda x: -x[1])
        main_skill, main_cnt = v[0]
        twins = sum(c for _, c in v[1:])
        print(f"  {main_skill:<18} {main_cnt:>5}  ← двойники: " +
              ", ".join(f"{show(s)} = {c}" for s, c in v[1:]) +
              f"  (+{twins / main_cnt:.0%} к частоте при склейке)")
    lost = sum(sum(c for _, c in v) - max(c for _, c in v) for v in mixed)
    print(f"\nУпоминаний у двойников: {lost} из {sum(freq.values()):,} ({lost / sum(freq.values()):.2%}).")
    print("Влияние на итоги ничтожно, но для отдельных навыков заметно: у «c#» двойник «с#» "
          "отнимает почти пятую часть упоминаний.")


if __name__ == "__main__":
    tee_log("data_quality")
    main()
