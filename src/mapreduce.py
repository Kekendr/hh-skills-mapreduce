"""Этап 3а. Собственный MapReduce: частоты навыков, пар и троек для варианта 2.

Драйвер делит объект в хранилище на сплиты по байтам, каждый сплит читает отдельный
процесс-маппер через S3 Range-запрос. Маппер отбирает вакансии варианта и считает сочетания
навыков, редьюсеры суммируют счётчики по партициям, результат пишется обратно в бакет.
Упавший маппер перезапускается, результат его неудачной попытки отбрасывается.

Запуск: python src/mapreduce.py
Тест отказа: python src/mapreduce.py --fail-map 3 --fail-mode crash
"""
import argparse
import csv
import hashlib
import io
import json
import os
import sys
import time
import multiprocessing as mp
import multiprocessing.connection as mp_connection
import zlib
from collections import Counter, deque
from concurrent.futures import ProcessPoolExecutor
from itertools import combinations

from config import (BUCKET, tee_log, RAW_KEY, RESULTS_DIR, RESULTS_PREFIX, VARIANT_KEYWORD,
                    VARIANT_NAME, VARIANT_ROLE, s3_client)

csv.field_size_limit(1 << 30)
TAIL_STEP = 64 * 1024          # шаг дочитывания строки за границей сплита
K_SIZES = (1, 2, 3)

_s3 = None                     # S3-клиент процесса-воркера


def _init_worker():
    """Создаёт S3-клиент в процессе-воркере."""
    global _s3
    _s3 = s3_client()


def _get_range(s3, lo, hi):
    """Читает байты [lo, hi] объекта через Range-запрос."""
    return s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f"bytes={lo}-{hi}")["Body"].read()


def read_split(s3, start, end, size):
    """Возвращает строки, которые начинаются в диапазоне [start, end).

    Строка принадлежит сплиту, в котором лежит её первый байт. Чтение идёт с start-1,
    чтобы понять, начинается ли сплит с новой строки; последняя строка дочитывается.
    """
    lo = start - 1 if start > 0 else 0
    data = _get_range(s3, lo, end - 1)
    fetched = len(data)
    if start > 0:
        nl = data.find(b"\n")
        if nl == -1:                       # весь сплит внутри чужой строки
            return b"", fetched
        data = data[nl + 1:]
    if not data:
        return b"", fetched
    if end < size and not data.endswith(b"\n"):
        pos = end
        while pos < size:
            tail = _get_range(s3, pos, min(pos + TAIL_STEP, size) - 1)
            fetched += len(tail)
            nl = tail.find(b"\n")
            if nl != -1:
                data += tail[: nl + 1]
                break
            data += tail
            pos += len(tail)
    return data, fetched


def iter_split_lines(s3, start, end, size, step=1 << 20):
    """То же, что read_split, но отдаёт строки по одной и не держит сплит в памяти."""
    lo = start - 1 if start > 0 else 0
    body = s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f"bytes={lo}-{end - 1}")["Body"]
    buf = b""
    skip_first = start > 0                 # начало сплита — хвост предыдущей строки
    for chunk in body.iter_chunks(step):
        buf += chunk
        if skip_first:
            nl = buf.find(b"\n")
            if nl == -1:
                buf = b""
                continue
            buf = buf[nl + 1:]
            skip_first = False
        *lines, buf = buf.split(b"\n")
        yield from lines
    if skip_first or not buf:              # в сплите не началось ни одной строки
        return
    pos = end                              # дочитываем последнюю строку за границей сплита
    while pos < size:
        tail = _get_range(s3, pos, min(pos + TAIL_STEP, size) - 1)
        nl = tail.find(b"\n")
        if nl != -1:
            buf += tail[:nl]
            break
        buf += tail
        pos += len(tail)
    yield buf


def normalize_skills(raw):
    """Приводит навыки к виду: уникальные, без пробелов по краям, в нижнем регистре."""
    return sorted({s.strip().lower() for s in raw.split(";") if s.strip()})


def is_variant(role, name, skills):
    """Проверяет условие варианта: нужная роль и Python в навыках или в названии."""
    return role == VARIANT_ROLE and (VARIANT_KEYWORD in skills or VARIANT_KEYWORD in name.lower())


def partition_of(key, n_reducers):
    """Возвращает номер партиции (редьюсера) для ключа."""
    return zlib.crc32("\x1f".join(key).encode("utf-8")) % n_reducers


# Байтовые образцы для быстрого предфильтра строк
ROLE_BYTES = VARIANT_ROLE.encode("utf-8")
KEYWORD_BYTES = VARIANT_KEYWORD.encode("ascii")


def map_task(task):
    """Маппер: обрабатывает один сплит и возвращает локальные счётчики по партициям."""
    idx, start, end, size, cols, n_red, attempt, fail_map, fail_mode, prefilter, stream = task
    t0 = time.perf_counter()

    if idx in fail_map and attempt == 1:   # отказ для теста
        if fail_mode == "crash":
            os._exit(17)                   # процесс завершается, не вернув результат
        raise RuntimeError(f"симулированный отказ маппера #{idx}")

    i_role, i_name, i_skills = cols
    parts = [[Counter() for _ in K_SIZES] for _ in range(n_red)]
    if stream:                             # оптимизация: потоковое чтение сплита
        lines = iter_split_lines(_s3, start, end, size)
        st = Counter()
    else:
        data, fetched = read_split(_s3, start, end, size)
        st = Counter(bytes_fetched=fetched, bytes_owned=len(data))
        lines = data.split(b"\n")

    for li, line in enumerate(lines):
        if not line:
            continue
        if idx == 0 and li == 0:           # заголовок CSV
            continue
        st["lines"] += 1
        # предфильтр: без байтов роли и слова python строка точно не подходит
        if prefilter and (ROLE_BYTES not in line or KEYWORD_BYTES not in line.lower()):
            continue
        st["parsed"] += 1
        rec = next(csv.reader([line.decode("utf-8")]))
        skills = normalize_skills(rec[i_skills])
        if not is_variant(rec[i_role], rec[i_name], skills):
            continue
        st["variant_vacancies"] += 1
        if not skills:
            continue
        st["with_skills"] += 1
        for ki, k in enumerate(K_SIZES):
            for combo in combinations(skills, k):        # навыки уже отсортированы
                parts[partition_of(combo, n_red)][ki][combo] += 1  # комбайнер внутри маппера
                st[f"emitted_k{k}"] += 1

    st["map_seconds"] = time.perf_counter() - t0
    return idx, attempt, parts, dict(st)


def reduce_task(args):
    """Редьюсер: суммирует счётчики одной партиции от всех мапперов."""
    r, inputs = args
    out = [Counter() for _ in K_SIZES]
    for per_k in inputs:
        for ki in range(len(K_SIZES)):
            out[ki].update(per_k[ki])
    return r, out


def _worker_loop(conn):
    """Цикл процесса-воркера: получает задачи по каналу, отправляет результат или ошибку."""
    _init_worker()
    while True:
        task = conn.recv()
        if task is None:
            break
        try:
            conn.send(("ok", map_task(task)))
        except Exception as e:  # любой сбой маппера передаётся драйверу
            conn.send(("error", repr(e)))


def run_map_phase(tasks, workers, max_attempts, log):
    """Выполняет map-задачи и перезапускает только упавшие.

    Если маппер бросил исключение или его процесс аварийно завершился, задача ставится
    на повтор, а на место погибшего процесса запускается новый. Остальные результаты сохраняются.
    """
    ctx = mp.get_context("spawn")
    queue = deque(tasks)
    attempt = {t[0]: 1 for t in tasks}
    done, failures = {}, []
    busy = {}                                   # канал -> (процесс, задача)
    idle = []

    def spawn():
        """Запускает новый процесс-воркер."""
        parent, child = ctx.Pipe()
        p = ctx.Process(target=_worker_loop, args=(child,), daemon=True)
        p.start()
        child.close()
        return p, parent

    def retry(task, reason):
        """Ставит задачу на повтор, после max_attempts попыток останавливает работу."""
        i = task[0]
        failures.append((i, attempt[i], reason))
        log(f"  ОТКАЗ маппера #{i}, попытка {attempt[i]}: {reason}; результат попытки отброшен")
        attempt[i] += 1
        if attempt[i] > max_attempts:
            raise SystemExit(f"Маппер #{i} не выполнен за {max_attempts} попыток")
        t = list(task)
        t[6] = attempt[i]
        queue.appendleft(tuple(t))
        log(f"  маппер #{i} поставлен на повтор, попытка {attempt[i]}")

    idle = [spawn() for _ in range(min(workers, len(tasks)))]
    while queue or busy:
        while queue and idle:
            proc, conn = idle.pop()
            task = queue.popleft()
            conn.send(task)
            busy[conn] = (proc, task)
        ready = mp_connection.wait(list(busy) + [p.sentinel for p, _ in busy.values()])
        for conn in list(busy):
            proc, task = busy[conn]
            if conn not in ready and proc.sentinel not in ready:
                continue
            try:
                status, payload = conn.recv()
            except (EOFError, OSError):          # процесс погиб, не отдав результат
                del busy[conn]
                proc.join()
                retry(task, f"процесс pid={proc.pid} аварийно завершился (код {proc.exitcode})")
                idle.append(spawn())             # замена погибшему воркеру
                continue
            del busy[conn]
            if status == "ok":
                i, _, parts, st = payload
                done[i] = (parts, st)
            else:
                retry(task, payload)
            idle.append((proc, conn))
    for proc, conn in idle:
        conn.send(None)
        proc.join()
    return done, failures, attempt


def to_rows(counter):
    """Сортирует по убыванию частоты, затем по ключу."""
    return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))


def canonical_digest(counters):
    """Считает sha256 результата — контрольное число для сверки с DuckDB."""
    h = hashlib.sha256()
    for k, c in zip(K_SIZES, counters):
        for key in sorted(c):
            h.update(f"{k}\t{chr(31).join(key)}\t{c[key]}\n".encode("utf-8"))
    return h.hexdigest()


def write_results(s3, counters, summary, engine, log):
    """Сохраняет результаты k=1,2,3 и сводку в бакет и в локальную папку."""
    out_dir = RESULTS_DIR / engine
    out_dir.mkdir(parents=True, exist_ok=True)
    names = {1: "skills", 2: "pairs", 3: "triples"}
    for k, c in zip(K_SIZES, counters):
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow([f"skill{j}" for j in range(1, k + 1)] + ["count"])
        for key, cnt in to_rows(c):
            w.writerow(list(key) + [cnt])
        body = buf.getvalue().encode("utf-8")
        fname = f"k{k}_{names[k]}.csv"
        (out_dir / fname).write_bytes(body)
        s3.put_object(Bucket=BUCKET, Key=f"{RESULTS_PREFIX}/{engine}/{fname}", Body=body,
                      ContentType="text/csv")
        log(f"  s3://{BUCKET}/{RESULTS_PREFIX}/{engine}/{fname:<18} {len(c):>9,} строк, {len(body):>12,} байт")
    js = json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8")
    (out_dir / "summary.json").write_bytes(js)
    s3.put_object(Bucket=BUCKET, Key=f"{RESULTS_PREFIX}/{engine}/summary.json", Body=js,
                  ContentType="application/json")
    log(f"  s3://{BUCKET}/{RESULTS_PREFIX}/{engine}/summary.json")


def main():
    """Делит объект на сплиты, выполняет map, shuffle и reduce, сохраняет результат."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--chunk-mb", type=float, default=32, help="размер сплита, МиБ")
    ap.add_argument("--workers", type=int, default=6, help="процессов-мапперов")
    ap.add_argument("--reducers", type=int, default=4, help="число партиций/редьюсеров")
    ap.add_argument("--max-attempts", type=int, default=3)
    ap.add_argument("--fail-map", default="", help="номера мапперов для инъекции отказа, через запятую")
    ap.add_argument("--fail-mode", choices=["exception", "crash"], default="exception")
    ap.add_argument("--no-write", action="store_true", help="не сохранять результаты (для профилирования)")
    ap.add_argument("--prefilter", action="store_true",
                    help="оптимизация: отсев строк по байтам до CSV-разбора")
    ap.add_argument("--stream", action="store_true",
                    help="оптимизация: потоковое чтение сплита вместо загрузки целиком")
    ap.add_argument("--inline-reduce", action="store_true",
                    help="оптимизация: reduce в процессе драйвера, без второго пула процессов")
    ap.add_argument("--optimized", action="store_true",
                    help="все оптимизации: --prefilter --stream --inline-reduce")
    ap.add_argument("--engine-name", default="mapreduce", help="каталог результатов в бакете")
    args = ap.parse_args()
    if args.optimized:
        args.prefilter = args.stream = args.inline_reduce = True
    fail_map = {int(x) for x in args.fail_map.split(",") if x.strip()}
    if fail_map:                                   # вывод сохраняется в results/logs/
        tee_log(f"mapreduce_fail_{args.fail_mode}")
    elif not args.no_write:
        tee_log("mapreduce_optimized" if args.optimized else
                "mapreduce_run" if args.engine_name == "mapreduce" else args.engine_name)

    def log(msg=""):
        print(msg, flush=True)

    t_all = time.perf_counter()
    s3 = s3_client()
    size = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)["ContentLength"]
    header = next(csv.reader([_get_range(s3, 0, 65535).split(b"\n", 1)[0].decode("utf-8")]))
    cols = (header.index("role"), header.index("name"), header.index("key_skills"))

    chunk = int(args.chunk_mb * 2**20)
    splits = [(i, s, min(s + chunk, size)) for i, s in enumerate(range(0, size, chunk))]
    tasks = [(i, s, e, size, cols, args.reducers, 1, fail_map, args.fail_mode, args.prefilter,
              args.stream) for i, s, e in splits]

    log(f"MapReduce | вариант 2: {VARIANT_NAME}")
    log(f"Источник: s3://{BUCKET}/{RAW_KEY} ({size:,} байт)")
    log(f"Сплитов: {len(splits)} по {args.chunk_mb:g} МиБ | мапперов: {args.workers} | "
        f"редьюсеров: {args.reducers}")
    opts = [n for n, on in [("предфильтр", args.prefilter), ("потоковое чтение", args.stream),
                            ("reduce в драйвере", args.inline_reduce)] if on]
    if opts:
        log(f"Оптимизации: {', '.join(opts)}")
    if fail_map:
        log(f"Инъекция отказа: мапперы {sorted(fail_map)}, режим «{args.fail_mode}»")

    # Map
    t0 = time.perf_counter()
    log("\n[map]")
    done, failures, attempts = run_map_phase(tasks, args.workers, args.max_attempts, log)
    t_map = time.perf_counter() - t0
    stats = Counter()
    for _, st in done.values():
        stats.update({k: v for k, v in st.items() if k != "map_seconds"})
    log(f"  завершено мапперов: {len(done)}/{len(splits)} за {t_map:.2f} с; "
        f"отказов: {len(failures)}")
    read_info = (f"прочитано из S3: {stats['bytes_fetched']:,} байт; " if not args.stream
                 else "чтение потоковое; ")
    log(f"  {read_info}строк данных: {stats['lines']:,}; разобрано CSV: {stats['parsed']:,}")

    # Shuffle и reduce
    t0 = time.perf_counter()
    log("\n[shuffle + reduce]")
    reduce_inputs = [(r, [done[i][0][r] for i in sorted(done)]) for r in range(args.reducers)]
    finals = [Counter() for _ in K_SIZES]

    def consume(results):
        for r, out in results:
            sizes = [len(c) for c in out]
            log(f"  редьюсер {r}: ключей k1={sizes[0]:,} k2={sizes[1]:,} k3={sizes[2]:,}")
            for ki in range(len(K_SIZES)):
                finals[ki].update(out[ki])     # партиции не пересекаются по ключам

    if args.inline_reduce:
        consume(map(reduce_task, reduce_inputs))
    else:
        with ProcessPoolExecutor(min(args.reducers, args.workers)) as ex:
            consume(ex.map(reduce_task, reduce_inputs))
    t_reduce = time.perf_counter() - t0
    log(f"  shuffle+reduce: {t_reduce:.2f} с")

    # Результат
    digest = canonical_digest(finals)
    summary = {
        "engine": args.engine_name,
        "variant": VARIANT_NAME,
        "source": f"s3://{BUCKET}/{RAW_KEY}",
        "splits": len(splits), "workers": args.workers, "reducers": args.reducers,
        "data_lines": stats["lines"],
        "variant_vacancies": stats["variant_vacancies"],
        "vacancies_with_skills": stats["with_skills"],
        "distinct": {f"k{k}": len(c) for k, c in zip(K_SIZES, finals)},
        "total_occurrences": {f"k{k}": sum(c.values()) for k, c in zip(K_SIZES, finals)},
        "emitted_by_mappers": {f"k{k}": stats[f"emitted_k{k}"] for k in K_SIZES},
        "map_failures": [list(map(str, f)) for f in failures],
        "sha256": digest,
        "seconds": {"map": round(t_map, 3), "shuffle_reduce": round(t_reduce, 3)},
    }
    log("\n[результат]")
    log(f"  вакансий варианта: {summary['variant_vacancies']:,}; из них с навыками: "
        f"{summary['vacancies_with_skills']:,}")
    for k, c in zip(K_SIZES, finals):
        log(f"  k={k}: уникальных {len(c):>9,} | сумма частот {sum(c.values()):>10,}")
    log(f"  контрольный sha256: {digest}")
    for k, c in zip(K_SIZES, finals):
        top = ", ".join(f"{' + '.join(key)} ({cnt})" for key, cnt in to_rows(c)[:3])
        log(f"  топ-3 k={k}: {top}")

    if not args.no_write:
        t0 = time.perf_counter()
        log("\n[запись результатов в хранилище]")
        summary["seconds"]["total"] = round(time.perf_counter() - t_all, 3)
        write_results(s3, finals, summary, args.engine_name, log)
        log(f"  запись: {time.perf_counter() - t0:.2f} с")
    log(f"\nИтого: {time.perf_counter() - t_all:.2f} с")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
