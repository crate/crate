
SELECT 
  ID AS "Run ID",
  TSTAMP AS "Run timestamp",
  JVM AS "JVM",
  OS AS "OS",
  CUSTOM_KEY AS "Custom key"
FROM RUNS R
WHERE R.ID = ?
