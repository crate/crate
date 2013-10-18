
INSERT INTO TESTS 
  (RUN_ID, CLASSNAME, NAME, BENCHMARK_ROUNDS, WARMUP_ROUNDS,
   ROUND_AVG, ROUND_STDDEV,
   GC_AVG, GC_STDDEV,
   GC_INVOCATIONS, GC_TIME,
   TIME_BENCHMARK, TIME_WARMUP
  ) 
VALUES
  (?,      ?,         ?,    ?,                ?,
   ?, ?,
   ?, ?,
   ?, ?,
   ?, ?);
  