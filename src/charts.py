"""Графики для отчёта, строятся по сохранённым результатам.

Запуск: python src/charts.py
"""
import csv
import json
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # импорт после выбора бэкенда Agg

from config import FIGURES_DIR, RESULTS_DIR

OUT = FIGURES_DIR
# Цвета проверены на различимость; синий — рост, красный — снижение
BLUE, ORANGE, AQUA, RED = "#2a78d6", "#eb6834", "#1baf7a", "#e34948"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
RAMP = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#0d366b"]   # шкала от светлого к тёмному

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "text.color": INK,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "axes.titlesize": 10,
    "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.spines.top": False,
    "axes.spines.right": False, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "legend.frameon": False,
    "figure.dpi": 200, "savefig.bbox": "tight",
})


def read_csv(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def sp(x):
    return f"{x:,.0f}".replace(",", " ")


def growth():
    """Рост числа сочетаний и вклад вакансий по числу навыков."""
    s = json.loads((RESULTS_DIR / "mapreduce" / "summary.json").read_text(encoding="utf-8"))
    ks = [1, 2, 3]
    distinct = [s["distinct"][f"k{k}"] for k in ks]
    total = [s["total_occurrences"][f"k{k}"] for k in ks]
    comb = read_csv(RESULTS_DIR / "analysis" / "combinatorics_by_skill_count.csv")

    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 3.6), gridspec_kw={"width_ratios": [1, 1.25], "wspace": 0.35})
    a.plot(ks, total, color=BLUE, lw=2, marker="o", ms=7, mec="white", mew=1.5)
    a.plot(ks, distinct, color=ORANGE, lw=2, marker="o", ms=7, mec="white", mew=1.5)
    a.set_yscale("log")
    a.set_xticks(ks, ["навыки (k=1)", "пары (k=2)", "тройки (k=3)"])
    a.set_xlim(0.7, 3.75)
    a.grid(axis="x", visible=False)
    for k, t, d in zip(ks, total, distinct):
        a.annotate(sp(t), (k, t), textcoords="offset points", ha="left" if k == 1 else "right",
                   xytext=(7, 6) if k == 1 else (-7, 6), color=INK)
        a.annotate(sp(d), (k, d), textcoords="offset points", ha="right" if k == 3 else "left",
                   xytext=(-7, -12) if k == 3 else (7, -11), color=INK)
    a.set_ylim(1500, 4e6)
    a.text(3.15, total[-1], "сумма\nчастот", color=BLUE, va="center", fontsize=8.5, fontweight="bold")
    a.text(3.15, distinct[-1] / 2.2, "уникальных\nсочетаний", color=ORANGE, va="center", fontsize=8.5,
           fontweight="bold")
    a.set_title("Рост числа сочетаний (лог. шкала)")

    buckets = [r["skills_bucket"] for r in comb]
    left = [0.0, 0.0, 0.0]
    for i, r in enumerate(comb):
        vals = [int(r["k1"]), int(r["k2"]), int(r["k3"])]
        shares = [v / t for v, t in zip(vals, total)]
        b.barh(range(3), shares, left=left, color=RAMP[i], edgecolor="white", linewidth=1.5,
               height=0.6, label=f"{buckets[i]} навыков")
        for y, (l, sh) in enumerate(zip(left, shares)):
            if sh >= 0.07:
                b.text(l + sh / 2, y, f"{sh:.0%}", ha="center", va="center", fontsize=8,
                       color="white" if i >= 2 else INK)
        left = [l + sh for l, sh in zip(left, shares)]
    b.set_yticks(range(3), ["навыки", "пары", "тройки"])
    b.invert_yaxis()
    b.set_xlim(0, 1)
    b.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    b.grid(axis="y", visible=False)
    b.legend(ncol=5, loc="upper center", bbox_to_anchor=(0.5, -0.12), fontsize=8, handlelength=1)
    b.set_title("Чей вклад в сочетания: вакансии по числу навыков")
    fig.savefig(OUT / "growth.png")
    plt.close(fig)


def profiling():
    """Время и пик памяти по конфигурациям."""
    rows = read_csv(RESULTS_DIR / "profiling" / "summary.csv")
    names = [r["config"] for r in rows]
    colors = [BLUE if n.startswith("MapReduce") else ORANGE for n in names]
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 3.0), sharey=True)
    y = range(len(rows))
    t = [float(r["time_mean"]) for r in rows]
    ts = [float(r["time_std"]) for r in rows]
    m = [float(r["mem_mean_mib"]) for r in rows]
    a.barh(y, t, xerr=ts, color=colors, height=0.6, error_kw={"ecolor": INK2, "capsize": 3, "lw": 1})
    b.barh(y, m, color=colors, height=0.6)
    for i in y:
        a.text(t[i] + ts[i] + 0.12, i, f"{t[i]:.2f} ± {ts[i]:.2f} с", va="center", fontsize=8)
        b.text(m[i] + 15, i, f"{m[i]:.0f} МиБ", va="center", fontsize=8)
    a.set_yticks(list(y), names)
    a.invert_yaxis()
    a.set_xlim(0, max(t) * 1.45)
    b.set_xlim(0, max(m) * 1.3)
    for ax in (a, b):
        ax.grid(axis="y", visible=False)
    a.set_title(f"Время, среднее ± ст. откл. ({rows[0]['runs']} запусков)")
    b.set_title("Пик памяти дерева процессов")
    handles = [matplotlib.patches.Patch(color=BLUE, label="собственный MapReduce"),
               matplotlib.patches.Patch(color=ORANGE, label="DuckDB")]
    fig.legend(handles=handles, ncol=2, loc="lower center", bbox_to_anchor=(0.5, -0.06))
    fig.savefig(OUT / "profiling.png")
    plt.close(fig)


def sweep():
    """Время и память MapReduce в зависимости от числа мапперов."""
    rows = read_csv(RESULTS_DIR / "profiling" / "sweep_workers.csv")
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 3.0))
    for mode, color in (("базовый", BLUE), ("оптимизир.", ORANGE)):
        rs = [r for r in rows if r["mode"] == mode]
        w = [int(r["workers"]) for r in rs]
        t = [float(r["time_mean"]) for r in rs]
        m = [float(r["peak_mib"]) for r in rs]
        label = "базовый" if mode == "базовый" else "оптимизир."
        a.plot(w, t, color=color, lw=2, marker="o", ms=6, mec="white", mew=1.2)
        b.plot(w, m, color=color, lw=2, marker="o", ms=6, mec="white", mew=1.2)
        a.text(w[-1] + 0.4, t[-1], label, color=color, va="center", fontsize=8.5, fontweight="bold")
        b.text(w[-1] + 0.4, m[-1], label, color=color, va="center", fontsize=8.5, fontweight="bold")
    for ax in (a, b):
        ax.set_xticks([1, 2, 4, 6, 12])
        ax.set_xlim(0.5, 16.5)
        ax.set_xlabel("число процессов-мапперов")
    a.set_ylim(0, None)
    b.set_ylim(0, None)
    a.set_title("Время MapReduce, с (3 запуска на точку)")
    b.set_title("Пик памяти MapReduce, МиБ")
    fig.savefig(OUT / "sweep.png")
    plt.close(fig)


def uplift():
    """Пары навыков с наибольшей прибавкой и наибольшим снижением медианы."""
    rows = read_csv(RESULTS_DIR / "analysis" / "pair_uplift.csv")
    rows.sort(key=lambda r: float(r["delta_pct"]), reverse=True)
    sel = rows[:8] + rows[-8:]
    labels = [f"{r['base']} + {r['added']}" for r in sel]
    vals = [float(r["delta_pct"]) * 100 for r in sel]
    fig, ax = plt.subplots(figsize=(10, 4.4))
    y = range(len(sel))
    ax.barh(y, vals, color=[BLUE if v > 0 else RED for v in vals], height=0.62)
    for i, (v, r) in enumerate(zip(vals, sel)):
        txt = f"{v:+.0f}%  ({sp(float(r['pair_median']))} ₽, n={r['pair_vacancies']})"
        ax.text(v + (2 if v > 0 else -2), i, txt, va="center", ha="left" if v > 0 else "right", fontsize=7.5)
    ax.set_yticks(list(y), labels)
    ax.invert_yaxis()
    ax.axvline(0, color=INK2, lw=0.8)
    ax.set_xlim(-130, 160)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("изменение медианы относительно базового (более частого) навыка, %")
    ax.set_title("Пары навыков: сильнейшие прибавки и снижения медианной зарплаты")
    fig.savefig(OUT / "uplift_pairs.png")
    plt.close(fig)


def regions():
    """Медиана зарплаты по опыту в трёх регионах."""
    rows = {r["metric"]: r for r in read_csv(RESULTS_DIR / "analysis" / "regions_key_metrics.csv")}
    regs = ["Москва", "Санкт-Петербург", "Екатеринбург"]
    groups = [("Нет опыта", "Медиана: нет опыта, ₽"), ("1–3 года", "Медиана: опыт 1–3 года, ₽"),
              ("3–6 лет", "Медиана: опыт 3–6 лет, ₽"), ("Все вакансии", "Медиана з/п на руки, ₽")]
    fig, ax = plt.subplots(figsize=(10, 3.2))
    width = 0.26
    for j, (reg, color) in enumerate(zip(regs, (BLUE, ORANGE, AQUA))):
        xs = [i + (j - 1) * width for i in range(len(groups))]
        vals = [float(rows[key][reg]) / 1000 for _, key in groups]
        ax.bar(xs, vals, width=width - 0.03, color=color, label=reg)
        for x, v in zip(xs, vals):
            ax.text(x, v + 3, f"{v:.0f}", ha="center", fontsize=7.5)
    ax.set_xticks(range(len(groups)), [g for g, _ in groups])
    ax.set_ylabel("тыс. ₽ на руки")
    ax.grid(axis="x", visible=False)
    ax.set_ylim(0, 255)
    ax.legend(ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.12))
    ax.set_title("Медиана зарплаты по требуемому опыту и регионам")
    fig.savefig(OUT / "regions_salary.png")
    plt.close(fig)


def main():
    """Строит все графики."""
    OUT.mkdir(parents=True, exist_ok=True)
    for fn in (growth, profiling, sweep, uplift, regions):
        fn()
        print(f"  {fn.__name__}")
    print(f"-> {OUT}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
