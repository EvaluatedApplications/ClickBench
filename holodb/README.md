# HoloDb

HoloDb is an embedded, commercially licensed .NET database: typed columns, exact aggregates, compiled scans. It runs in the application's process; there is no server.

HoloDb is commercial software. Its licensor permits running ClickBench against it and publishing the results.

## How it runs
- `install` installs the .NET 8 SDK and builds `tool/` (loader and single-query CLI, about 300 lines of C#) against the public `EvaluatedApplications.HoloDb` NuGet package, version 18.1.7.
- `load` reads the official `hits.parquet` and applies the official conversions (`make_date` / `epoch_ms` on the four time columns), then bulk-loads it into one table. The parquet is read with DuckDB's reader inside the loader. The table is the official `create.sql`, unchanged, with no primary key.
- `query` is a new process per run. Its reported time covers `Database.Open`, `Database.Execute` and rendering the result rows; the .NET runtime start-up before `Main` is not included. The driver's `SELECT 1` start-up probe has no FROM clause, which the engine does not parse, so the tool answers a bare integer constant itself after opening the database; no benchmark query takes this path.
- `queries.sql` and `create.sql` are the official DuckDB ones, byte for byte.

## Settings and structures
- Untuned: no index, summary, hint or configuration setting is declared or changed. The tool sets no .NET runtime options either (default garbage collector and JIT settings).
- HoloDb keeps an exact total per column for every table it holds (count, sum, min, max). Queries 1, 3, 4 and 7 are whole-table COUNT/SUM/AVG/MIN/MAX and are answered from those totals without reading rows. Nothing was created for this benchmark.
- HoloDb can also build structures from how a table is queried. The benchmark process exits straight after its one query, so nothing built during a run is saved for the next run.
- `BENCH_RESTARTABLE=no`: there is no daemon; every run is a fresh process after the page cache is dropped.