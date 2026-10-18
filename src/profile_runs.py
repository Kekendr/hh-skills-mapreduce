"""Профилирование времени и пика памяти: каждая конфигурация запускается несколько раз.

Каждый запуск — отдельный процесс. Время меряется от старта до завершения процесса,
память — как пик суммарного RSS всех его процессов, опрос каждые 20 мс.

Запуск: python src/profile_runs.py [--runs 5] [--sweep]
"""
import argparse
import csv
import io
import statistics as stats
import subprocess
import sys
import threading
import time

from pathlib import Path

import psutil

from config import tee_log, BUCKET, RESULTS_DIR, RESULTS_PREFIX, s3_client

PY = sys.executable
SRC = Path(__file__).resolve().parent
CONFIGS = [
    ("MapReduce (базовый)", [PY, str(SRC / "mapreduce.py"), "--no-write"]),
    ("MapReduce + предфильтр", [PY, str(SRC / "mapreduce.py"), "--no-write", "--prefilter"]),
    ("MapReduce оптимизир.", [PY, str(SRC / "mapreduce.py"), "--no-write", "--optimized",
                              "--workers", "4"]),
    ("DuckDB по CSV", [PY, str(SRC / "duckdb_counts.py"), "--no-write"]),
    ("DuckDB по Parquet", [PY, str(SRC / "duckdb_counts.py"), "--no-write", "--source", "parquet"]),
]


def run_once(cmd):
    """Запускает команду и возвращает время, пик памяти дерева процессов и код выхода."""
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    root = psutil.Process(proc.pid)
    peak, stop = 0, threading.Event()

    def sample():
        """Опрашивает память процесса и его потомков до завершения."""
        nonlocal peak
        while not stop.is_set():
            try:
                tree = [root] + root.children(recursive=True)
                rss = 0
                for p in tree:
                    try:
                        rss += p.memory_info().rss
                    except psutil.Error:
                        pass
                peak = max(peak, rss)
            except psutil.Error:
                pass
            time.sleep(0.02)

    th = threading.Thread(target=sample, daemon=True)
    th.start()
    _, err = proc.communicate()
    dt = time.perf_counter() - t0
    stop.set()
    th.join()
    if proc.returncode:
        print(err.decode("utf-8", "replace")[-2000:])
    return dt, peak, proc.returncode


def summarize(name, rows):
    """Считает среднее, отклонение и пик памяти по запускам."""
    ts = [r[0] for r in rows]
    ms = [r[1] / 2**20 for r in rows]
    return {
        "config": name, "runs": len(rows),
        "time_mean": stats.mean(ts), "time_std": stats.stdev(ts) if len(ts) > 1 else 0.0,
        "time_min": min(ts), "time_max": max(ts),
        "mem_mean_mib": stats.mean(ms), "mem_max_mib": max(ms),
    }


def save(name, header, rows):
    """Сохраняет таблицу замеров в бакет и в локальную папку."""
    out = RESULTS_DIR / "profiling"
    out.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    body = buf.getvalue().encode("utf-8")
    (out / f"{name}.csv").write_bytes(body)
    s3_client().put_object(Bucket=BUCKET, Key=f"{RESULTS_PREFIX}/profiling/{name}.csv", Body=body)
    print(f"  -> s3://{BUCKET}/{RESULTS_PREFIX}/profiling/{name}.csv")


def main():
    """Прогоняет все конфигурации и печатает сводку."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--sweep", action="store_true", help="масштабирование MapReduce по числу мапперов")
    args = ap.parse_args()
    cpu = psutil.cpu_count(logical=False), psutil.cpu_count()
    print(f"Профилирование: {args.runs} запусков на конфигурацию | CPU {cpu[0]} физ./{cpu[1]} лог. ядер | "
          f"RAM {psutil.virtual_memory().total / 2**30:.1f} GiB\n")

    all_runs, summaries = [], []
    for name, cmd in CONFIGS:
        rows = []
        for i in range(1, args.runs + 1):
            dt, peak, code = run_once(cmd)
            rows.append((dt, peak))
            all_runs.append((name, i, round(dt, 3), round(peak / 2**20, 1), code))
            print(f"  {name:<24} запуск {i}: {dt:6.2f} с | пик памяти {peak / 2**20:7.1f} МиБ"
                  f"{'' if code == 0 else f' | КОД {code}'}")
        s = summarize(name, rows)
        summaries.append(s)
        print(f"  {'':<24} среднее {s['time_mean']:6.2f} ± {s['time_std']:.2f} с | "
              f"пик {s['mem_mean_mib']:7.1f} МиБ (макс {s['mem_max_mib']:.1f})\n")

    base = {s["config"]: s for s in summaries}
    print("Сводка:")
    print(f"  {'конфигурация':<24} | {'время, с':>15} | {'мин, с':>7} | {'пик памяти, МиБ':>15}")
    for s in summaries:
        print(f"  {s['config']:<24} | {s['time_mean']:7.2f} ± {s['time_std']:5.2f} | {s['time_min']:7.2f} | "
              f"{s['mem_mean_mib']:15.1f}")
    for before, after in [("MapReduce (базовый)", "MapReduce + предфильтр"),
                          ("MapReduce (базовый)", "MapReduce оптимизир."),
                          ("DuckDB по CSV", "DuckDB по Parquet"),
                          ("MapReduce (базовый)", "DuckDB по CSV"),
                          ("MapReduce оптимизир.", "DuckDB по Parquet")]:
        b, a = base[before], base[after]
        print(f"  {before} -> {after}: время ×{b['time_mean'] / a['time_mean']:.2f}, "
              f"память ×{b['mem_mean_mib'] / a['mem_mean_mib']:.2f}")
    save("runs", ["config", "run", "seconds", "peak_mib", "exit_code"], all_runs)
    save("summary", list(summaries[0].keys()),
         [[round(v, 3) if isinstance(v, float) else v for v in s.values()] for s in summaries])

    if args.sweep:
        print("\nМасштабирование MapReduce: базовый и оптимизированный, по числу мапперов:")
        sweep = []
        for w in (1, 2, 4, 6, 12):
            for mode, extra in (("базовый", []), ("оптимизир.", ["--optimized"])):
                cmd = [PY, str(SRC / "mapreduce.py"), "--no-write", "--workers", str(w), *extra]
                rows = [run_once(cmd)[:2] for _ in range(3)]
                s = summarize(f"{mode} workers={w}", rows)
                sweep.append((mode, w, round(s["time_mean"], 3), round(s["time_std"], 3),
                              round(s["mem_mean_mib"], 1)))
                print(f"  {mode:<11} мапперов {w:>2}: {s['time_mean']:6.2f} ± {s['time_std']:.2f} с | "
                      f"пик {s['mem_mean_mib']:7.1f} МиБ")
        save("sweep_workers", ["mode", "workers", "time_mean", "time_std", "peak_mib"], sweep)


if __name__ == "__main__":
    tee_log("profiling")
    main()
