#!/bin/bash
# HoloDb is an embedded .NET library: like the DuckDB CLI, every query is a fresh process, so there is nothing to start, stop or restart.
export BENCH_DOWNLOAD_SCRIPT="download-hits-parquet-single"
export BENCH_RESTARTABLE=no
export BENCH_CONCURRENT_DURATION="${BENCH_CONCURRENT_DURATION:-0}"
exec ../lib/benchmark-common.sh