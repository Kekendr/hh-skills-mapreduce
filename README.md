# Навыки Python-разработчиков: MapReduce и DuckDB поверх RustFS

Домашнее задание №3 по курсу инженерии данных.
Автор: Шайкин Андрей Сергеевич, ИСУ 468044, группа ИИИ-3-ИнжДан-1-3.
Вариант 2 — «Программист, разработчик на Python».

Описание решения, результаты и выводы — в отчёте [`report/Шайкин_дз3.pdf`](report/Шайкин_дз3.pdf).
Здесь — как запустить.

## Запуск

Нужны Python 3.11+, S3-совместимое хранилище и файл `it_vacancies_2024.csv`
в каталоге уровнем выше репозитория.

### Хранилище

Работа выполнялась на такой конфигурации: Windows 10, RustFS 1.0.1, четыре папки-диска
на одном физическом диске `D:`, ключи доступа по умолчанию. Команда запуска сервера для этого стенда:

```powershell
$env:RUSTFS_ACCESS_KEY='rustfsadmin'; $env:RUSTFS_SECRET_KEY='rustfsadmin'
$env:RUSTFS_UNSAFE_BYPASS_DISK_CHECK='true'
rustfs.exe server --address 127.0.0.1:9000 --console-enable --console-address 127.0.0.1:9001 `
    D:\rustfs\data1 D:\rustfs\data2 D:\rustfs\data3 D:\rustfs\data4
```

`RUSTFS_UNSAFE_BYPASS_DISK_CHECK` понадобился только потому, что все папки лежат на одном
физическом диске — иначе RustFS не запускается. На другой машине пути к дискам, ключи и адрес
будут свои; скриптам они передаются переменными `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`.

### Скрипты

```powershell
pip install -r requirements.txt

# Этап 2. Загрузка
python src/upload.py
python src/export_segment.py

# Этапы 3–4. Подсчёт двумя способами и сверка
python src/mapreduce.py
python src/mapreduce.py --fail-map 3 --fail-mode crash --engine-name mapreduce_failtest
python src/duckdb_counts.py
python src/verify.py mapreduce duckdb

# Этапы 5–6. Аналитика
python src/salary.py
python src/regions.py
python src/combinatorics.py
python src/data_quality.py

# Бонус и профилирование
python src/to_parquet.py
python src/profile_runs.py --runs 5 --sweep
python src/charts.py

# Тесты
python tests/test_splits.py          # разбиение на сплиты (без хранилища)
python tests/crosscheck_analysis.py  # независимая перепроверка этапов 5–6
```

Основные результаты скрипты пишут в бакет `hh-lake`. Локальные копии, логи запусков и графики
сохраняются в `results/` и `report/figures/` — в репозиторий они не входят, все выводы команд,
таблицы и графики приведены в отчёте. Каталоги можно перенести переменными `HH_RESULTS_DIR`
и `HH_FIGURES_DIR`.
