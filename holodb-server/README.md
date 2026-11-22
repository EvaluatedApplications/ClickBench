# HoloDb (server)

HoloDb as a service. The `HoloDb.Server` image runs the database in a container; the store is a host directory bind-mounted at `/data`. Each query is one round trip from a small .NET client over TLS, timed by the client. The server opens the store once in the untimed start, so the process and its JIT stay warm between queries.

HoloDb is commercial software. Its licensor permits running ClickBench against it and publishing the results.

## How it runs
- `install` installs Docker (`docker.io`) and pulls the image, then builds `tool/` (the loader and the client) with the .NET 8 SDK.
- `load` stops the container, runs the library loader on the official `hits.parquet` (DuckDB's reader inside the loader, the official conversions `make_date` and `epoch_ms`), and writes `holodb-data/holo` and `holodb-data/holo.tables`. The database is closed cleanly, so the next open skips the invariant walks. Then it starts the container on the new store. The restart happens inside the Load window; it is one server open, not a query cost.
- `start` runs `docker run -d --name holodb-bench` with the store bind-mounted at `/data`, the token mounted as a file, and the flags below. It returns when the TLS listener accepts connections and the dev certificate thumbprint is known.
- `stop` runs `docker stop -t 120` (SIGTERM, so the server closes the database and writes its clean-close stamp), then removes the container and waits until it is gone.
- `check` is a TLS connection pinned to the server's certificate, authenticated with the token, and a `Ping`. The protocol answers `Ping` without a table, so the probe works before the first load. A SQL probe cannot run before the load because `hits` does not exist yet.
- `query` reads one statement on stdin. It rewrites the official `STRLEN(` to `OCTET_LENGTH(` (the same byte count, which the standard dialect spells this way), starts a stopwatch around `ExecuteAsync` and the row rendering, and prints the rows on stdout. The last stderr line is the elapsed seconds, three decimals. Connection setup and the TLS handshake are not timed.
- `data-size` is `du -sb holodb-data`.
- `queries.sql` and `create.sql` are the official files, byte for byte, the same as `holodb/`.
- `bench.env` holds the pins: `HOLODB_VERSION` (the image tag and the `EvaluatedApplications.HoloDb` package the loader uses), `HOLODB_IMAGE`, the container name, the port, and the statement limit.

### Testing an image built from the working tree
```bash
# from the repo root
docker build -f HoloDb.Server/Dockerfile -t holodb:local .
HOLODB_IMAGE=holodb:local ./benchmark.sh
```
The loader still uses the `HOLODB_VERSION` package. The store format must match the image, so build the image from the same HoloDb version as that package.

## Container and file ownership
- The image's `ENTRYPOINT` runs `dotnet HoloDb.Server.dll` as the image user `app` (the .NET base image convention, uid 1654). `start` passes `--user "$(id -u):$(id -g)"`, so the container runs as the same uid and gid as the user who ran `load`. The loader's files are therefore owned by the container user: no `chown` and no umask change are needed.
- If a container is started some other way (for example with the image's default user), run `sudo chown -R 1654:1654 holodb-data` after `load` instead.
- The token file is `run/token`, mode 600, created by `start` the first time.

## Server flags
Every flag that differs from the server's default, and why.

| flag | default | value here | why |
|---|---|---|---|
| `--wal /data/holo` | in-memory | the store path | the store must be on disk; the paged layout is `holo` plus `holo.tables` |
| `--paged` | off | on | opens the paged store the loader writes (the server also detects an existing `holo.tables` directory by itself; the flag is passed to be explicit). |
| `--dialect extended` | standard | extended | the official queries use `LIMIT`, `DATE_TRUNC` and `REGEXP_REPLACE`, which only the extended dialect accepts |
| `--token-file /run/holodb/token` | none | the mounted token | authentication; the token travels in a file, not on the command line (visible to `docker inspect` and the process list) |
| `--statement-seconds 600` | 30 | 600 | a cold statement runs after a restart and a page-cache drop; 30 s would stop long scans. The limit is a guard, not a measurement |
| `DOTNET_gcServer=1` (environment) | off | on | server garbage collector for the throughput workload. The published server already sets it in its runtime config, so this is explicit only |

Not changed, and why:
- `--result-limit-mb` stays at 16. The largest answer on the 1M store is 8,126 bytes (q24). Results are bounded by `LIMIT` and the aggregates return one row; a 100M run returns the same shapes.
- `--memory-budget-mb` stays at 1024. It bounds request frames and BulkLoad arrays, not query results. This entry sends no BulkLoad.
- `--host` and `--port` are the image's (`0.0.0.0:5432` inside the container). The host maps `127.0.0.1:5432` only.
- No index, no summary, no hint, no configuration setting. The client sends the official SQL and nothing else.

## Status
- Tested on a c6a.4xlarge (Ubuntu, 16 vCPU, 30 GB RAM, 500 GB gp2) with the published image `ghcr.io/evaluatedapplications/holodb:22.0.0` and the packages `EvaluatedApplications.HoloDb 22.0.0` and `EvaluatedApplications.HoloDb.Client 2.0.2`, using `./install` and `./benchmark.sh` as they are in this folder. All 43 queries returned results on every try.
- Load 369 s, data size 40.5 GB (`holodb-data`), hot and cold sums of the 43 queries 14.9 s and 171 s (best of tries 2 and 3 for hot; try 1 after the framework's restart and cache drop for cold).
- Answers on the first 1,000,000 rows of `hits.parquet` match DuckDB's for every query whose result is defined by the SQL (ties at a `LIMIT` cut-off and one `LIMIT` without `ORDER BY` excepted).
- `chmod +x` is needed on the scripts in this folder if the checkout does not keep the executable bit.
## Learned structures
HoloDb builds structures from how a table is queried, automatically and with no setting: zone maps, per-column value counts (computed during the load), and learned predicate records over text columns (which distinct strings in a column match a predicate such as `URL LIKE '%google%'`). The structures are kept as data beside the table, bounded in size, and dropped when the table changes. Rows are always scanned and aggregated from the data on every statement: nothing stores a query's answer.
Because the predicate records are saved beside the table, a record built by one query is available to a later query after the framework's restart and cache drop, so a few cold tries (for example q22 and q24 after q21) are faster than a first-ever evaluation. The store is created fresh by `./load` for every run, so nothing carries over from an earlier run.
## Benchmark settings
- Untuned. No index, summary or hint. `BENCH_RESTARTABLE=yes` (a daemon: the cold cycle is stop, drop caches, start, check, query). `BENCH_CONCURRENT_DURATION=0`: the concurrent-QPS test is skipped for this entry, as specified.
- Data size is the directory `holodb-data` after load.
