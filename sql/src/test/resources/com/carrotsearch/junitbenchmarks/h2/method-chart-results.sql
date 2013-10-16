
SELECT 
  NAME AS "method",
  ROUND_AVG AS "Average time [s]",
  ROUND_STDDEV AS "StdDev",
  ROUND(GC_TIME, 2) AS "GC time [s]",
  GC_AVG, 2 AS "GC average [s]",
  GC_STDDEV AS "StdDev",
  GC_INVOCATIONS AS "GC calls",
  BENCHMARK_ROUNDS AS "benchmark rounds",
  WARMUP_ROUNDS AS "warmup rounds",
  TIME_BENCHMARK AS "Total benchmark time",
  TIME_WARMUP AS "Total warmup time"
FROM TESTS T, RUNS R
WHERE R.ID = ?
  AND T.RUN_ID = R.ID
  AND CLASSNAME = ?
