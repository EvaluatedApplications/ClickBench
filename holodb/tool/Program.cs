// HoloDb ClickBench tool: a loader and a single-query CLI over the public EvaluatedApplications.HoloDb package.
//   load  <db dir> <hits.parquet> <create.sql> [rows]   official parquet + official conversions into one HoloDb table (optionally the first `rows` rows)
//   query <db dir> <sql file>                    one query: result on stdout, elapsed seconds on the last stderr line
//   version
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using DuckDB.NET.Native;
using HoloDb;

return args.Length == 0 ? Usage() : args[0] switch
{
    "load" when args.Length >= 4 => Loader.Run(args[1], args[2], args[3], args.Length > 4 ? long.Parse(args[4]) : long.MaxValue),
    "query" when args.Length == 3 => Runner.Run(args[1], args[2]),
    "version" => Version(),
    _ => Usage()
};

static int Usage() { Console.Error.WriteLine("load <db dir> <hits.parquet> <create.sql> | query <db dir> <sql file> | version"); return 2; }
static int Version() { Console.WriteLine("holodb " + typeof(Database).Assembly.GetName().Version); return 0; }

internal static class Runner
{
    public static int Run(string dir, string sqlFile)
    {
        string sql = File.ReadAllText(sqlFile).Trim();
        var sw = Stopwatch.StartNew();
        var db = Database.Open(Path.Combine(dir, "holo"), 8L * 1024 * 1024, paged: true);
        // The engine has no SELECT without FROM. The driver's startup probe is `SELECT 1`: the tool answers a bare
        // integer constant itself, after opening the database, so the probe still measures process start plus Open.
        var k = System.Text.RegularExpressions.Regex.Match(sql, @"^SELECT\s+(-?\d{1,18})\s*;?$", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (k.Success)
        {
            Console.Out.Write(k.Groups[1].Value + "\n");
            sw.Stop();
            Console.Error.WriteLine(sw.Elapsed.TotalSeconds.ToString("F3", CultureInfo.InvariantCulture));
            Environment.Exit(0);
            return 0;
        }
        var r = db.Execute(sql);
        int nc = r.Columns.Count;
        var sb = new StringBuilder();
        for (int i = 0; i < r.RowCount; i++)
        {
            for (int c = 0; c < nc; c++)
            {
                if (c > 0) sb.Append('\t');
                var v = r.GetValue(i, c);
                sb.Append(v switch { null => "NULL", DBNull => "NULL", double d => d.ToString("R", CultureInfo.InvariantCulture), IFormattable f => f.ToString(null, CultureInfo.InvariantCulture), _ => v.ToString() ?? "" });
            }
            sb.Append('\n');
        }
        Console.Out.Write(sb.ToString());
        sw.Stop();
        Console.Error.WriteLine(sw.Elapsed.TotalSeconds.ToString("F3", CultureInfo.InvariantCulture));
        Environment.Exit(0);
        return 0;
    }
}

internal static unsafe class Loader
{
    sealed record Col(string Name, string Type);

    static List<Col> ParseCreate(string sql)
    {
        int a = sql.IndexOf('('), b = sql.LastIndexOf(')');
        var cols = new List<Col>();
        foreach (var part in sql.Substring(a + 1, b - a - 1).Split(','))
        {
            var t = part.Trim(); if (t.Length == 0) continue;
            int sp = t.IndexOfAny(new[] { ' ', '\t', '\r', '\n' });
            cols.Add(new Col(t[..sp], t[(sp + 1)..].Trim()));
        }
        return cols;
    }

    sealed class ByteBuf
    {
        public byte[] B = new byte[1 << 16]; public int N;
        public void Add(byte* p, int len)
        {
            if (N + len > B.Length) Array.Resize(ref B, (int)Math.Max((long)B.Length * 2, (long)N + len));
            new ReadOnlySpan<byte>(p, len).CopyTo(B.AsSpan(N)); N += len;
        }
    }

    sealed class Block { public int Rows; public object[] Data = null!; }

    sealed class Source : IDisposable
    {
        readonly DuckDBDatabase _db; readonly DuckDBNativeConnection _con; readonly string _from; readonly List<Col> _cols;
        public Source(string parquet, List<Col> cols)
        {
            _cols = cols; _from = parquet.Replace('\\', '/');
            if (NativeMethods.Startup.DuckDBOpen(null, out _db) != DuckDBState.Success) throw new Exception("duckdb open failed");
            if (NativeMethods.Startup.DuckDBConnect(_db, out _con) != DuckDBState.Success) throw new Exception("duckdb connect failed");
        }
        public void Dispose() { _con.Dispose(); _db.Dispose(); }

        public long Count()
        {
            if (NativeMethods.Query.DuckDBQuery(_con, $"SELECT COUNT(*) FROM read_parquet('{_from}')", out var r) != DuckDBState.Success) throw new Exception("count failed");
            try { var ch = NativeMethods.Query.DuckDBFetchChunk(r); var v = NativeMethods.DataChunks.DuckDBDataChunkGetVector(ch, 0); long n = *(long*)NativeMethods.Vectors.DuckDBVectorGetData(v); ch.Dispose(); return n; }
            finally { r.Close(); }
        }

        string SelectList()
        {
            var sb = new StringBuilder();
            foreach (var c in _cols)
            {
                if (sb.Length > 0) sb.Append(", ");
                string e = c.Name switch
                {
                    "EventDate" => "make_date(EventDate)",
                    "EventTime" => "epoch_ms(EventTime * 1000)",
                    "ClientEventTime" => "epoch_ms(ClientEventTime * 1000)",
                    "LocalEventTime" => "epoch_ms(LocalEventTime * 1000)",
                    _ => c.Name
                };
                sb.Append($"CAST({e} AS {CastType(c.Type)}) AS {c.Name}");
            }
            return sb.ToString();
        }
        static string CastType(string t)
        {
            t = t.ToUpperInvariant().Replace("NOT NULL", "").Trim();
            return t.StartsWith("TEXT") || t.StartsWith("VARCHAR") || t.StartsWith("CHAR") ? "VARCHAR" : t;
        }

        public Block Read(long from, long to)
        {
            string sql = $"SELECT {SelectList()} FROM (SELECT *, file_row_number AS _frn FROM read_parquet('{_from}', binary_as_string=True, file_row_number=true)) WHERE _frn >= {from} AND _frn < {to}";
            if (NativeMethods.Query.DuckDBQuery(_con, sql, out var result) != DuckDBState.Success) throw new Exception("duckdb query failed: " + sql);
            try
            {
                int nc = _cols.Count; int cap = (int)(to - from);
                var data = new object[nc]; var bufs = new ByteBuf?[nc]; var offs = new int[nc][];
                for (int c = 0; c < nc; c++)
                {
                    string ty = _cols[c].Type.ToUpperInvariant();
                    if (ty.StartsWith("SMALLINT")) data[c] = new short[cap];
                    else if (ty.StartsWith("INTEGER") || ty.StartsWith("INT ") || ty == "INT" || ty.StartsWith("DATE")) data[c] = new int[cap];
                    else if (ty.StartsWith("BIGINT") || ty.StartsWith("TIMESTAMP")) data[c] = new long[cap];
                    else { offs[c] = new int[cap + 1]; bufs[c] = new ByteBuf(); }
                }
                int row = 0;
                while (true)
                {
                    var ch = NativeMethods.Query.DuckDBFetchChunk(result);
                    if (ch.IsInvalid) break;
                    int n = (int)NativeMethods.DataChunks.DuckDBDataChunkGetSize(ch);
                    if (n == 0) { ch.Dispose(); break; }
                    for (int c = 0; c < nc; c++)
                    {
                        var vec = NativeMethods.DataChunks.DuckDBDataChunkGetVector(ch, c);
                        void* p = NativeMethods.Vectors.DuckDBVectorGetData(vec);
                        ulong* valid = NativeMethods.Vectors.DuckDBVectorGetValidity(vec);
                        if (valid != null) for (int i = 0; i < n; i++) if ((valid[i >> 6] & (1UL << (i & 63))) == 0) throw new Exception($"NULL in column {_cols[c].Name}");
                        switch (data[c])
                        {
                            case short[] s: new ReadOnlySpan<short>(p, n).CopyTo(s.AsSpan(row, n)); break;
                            case int[] iv: new ReadOnlySpan<int>(p, n).CopyTo(iv.AsSpan(row, n)); break;
                            case long[] l: new ReadOnlySpan<long>(p, n).CopyTo(l.AsSpan(row, n)); break;
                            default:
                                {
                                    var bb = bufs[c]!; var of = offs[c]; byte* sp = (byte*)p;
                                    for (int i = 0; i < n; i++)
                                    {
                                        byte* e = sp + (long)i * 16; uint len = *(uint*)e;
                                        byte* chars = len <= 12 ? e + 4 : *(byte**)(e + 8);
                                        bb.Add(chars, (int)len); of[row + i + 1] = bb.N;
                                    }
                                    break;
                                }
                        }
                    }
                    row += n; ch.Dispose();
                }
                for (int c = 0; c < nc; c++) if (data[c] == null) data[c] = new Utf8Column(bufs[c]!.B, offs[c]);
                return new Block { Rows = row, Data = data };
            }
            finally { result.Close(); }
        }
    }

    public static int Run(string dir, string parquet, string createFile, long maxRows)
    {
        const int range = 1 << 18;
        var create = File.ReadAllText(createFile);
        var cols = ParseCreate(create);
        if (Directory.Exists(dir)) { Console.Error.WriteLine($"{dir} exists: delete it first"); return 1; }
        Directory.CreateDirectory(dir);
        var sw = Stopwatch.StartNew();
        var db = Database.Open(Path.Combine(dir, "holo"), 8L * 1024 * 1024, paged: true);
        db.Execute(create);
        var names = cols.Select(c => c.Name).ToArray();
        using var ds = new Source(parquet, cols);
        long total = Math.Min(ds.Count(), maxRows);
        Console.WriteLine($"loading {total:N0} rows x {cols.Count} columns");
        var q = new BlockingCollection<Block>(1);
        var prod = new Thread(() =>
        {
            try { for (long from = 0; from < total; from += range) q.Add(ds.Read(from, Math.Min(from + range, total))); }
            catch (Exception ex) { Console.Error.WriteLine("reader failed: " + ex); Environment.Exit(1); }
            finally { q.CompleteAdding(); }
        }) { IsBackground = true };
        prod.Start();
        long done = 0;
        foreach (var blk in q.GetConsumingEnumerable())
        {
            db.BulkLoadColumns("hits", names, blk.Data, blk.Rows);
            done += blk.Rows;
            if ((done / range) % 40 == 0) Console.WriteLine($"  {done:N0} rows  {sw.Elapsed.TotalSeconds:F1}s");
        }
        db.Close();
        Console.WriteLine($"loaded {done:N0} rows in {sw.Elapsed.TotalSeconds:F1}s");
        return 0;
    }
}
