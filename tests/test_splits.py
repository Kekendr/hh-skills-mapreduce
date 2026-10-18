"""Тест разбиения объекта на сплиты.

Проверяет, что при любом размере сплита каждая строка попадает ровно в один сплит
и что потоковое и обычное чтение сплита дают одинаковые строки.

Запуск: python tests/test_splits.py
"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import mapreduce as mr  # после добавления src в путь


class FakeBody:
    """Имитация тела ответа S3."""
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def iter_chunks(self, step):
        for i in range(0, len(self.data), step):
            yield self.data[i:i + step]


class FakeS3:
    """Имитация хранилища: отдаёт диапазоны байт из строки."""

    def __init__(self, data):
        self.data = data

    def get_object(self, Bucket, Key, Range):  # имена аргументов как у boto3
        lo, hi = map(int, Range.split("=")[1].split("-"))
        return {"Body": FakeBody(self.data[lo:hi + 1])}


def lines_whole(s3, size, chunk):
    """Собирает строки всех сплитов обычным чтением."""
    out = []
    for start in range(0, size, chunk):
        part, _ = mr.read_split(s3, start, min(start + chunk, size), size)
        out += [line for line in part.split(b"\n") if line]
    return out


def lines_stream(s3, size, chunk, step):
    """Собирает строки всех сплитов потоковым чтением."""
    out = []
    for start in range(0, size, chunk):
        out += [line for line in mr.iter_split_lines(s3, start, min(start + chunk, size), size, step)
                if line]
    return out


def main():
    """Проверяет разные размеры сплитов и порций чтения."""
    random.seed(1)
    lines = [("x" * random.randint(0, 300) + str(i)).encode() for i in range(2000)]
    mr.TAIL_STEP = 7                                  # мелкий шаг, чтобы дочитывание повторялось
    failures = 0
    for trailing_newline in (True, False):
        data = b"\n".join(lines) + (b"\n" if trailing_newline else b"")
        s3, size = FakeS3(data), len(data)
        for chunk in (1, 2, 3, 7, 50, 333, 4096, size - 1, size, size + 10):
            whole = lines_whole(s3, size, chunk)
            ok = whole == lines
            for step in (1, 5, 64, 1 << 20):
                ok &= lines_stream(s3, size, chunk, step) == lines
            failures += not ok
            print(f"финальный \\n={str(trailing_newline):<5} сплит={chunk:>6} байт: "
                  f"{'OK' if ok else 'ОШИБКА'} ({len(whole)} строк)")
    print("ИТОГ:", "все проверки пройдены" if not failures else f"ошибок: {failures}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    from config import tee_log
    tee_log("test_splits")
    main()
