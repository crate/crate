SELECT * FROM t1 WHERE EXISTS ((VALUES 1) INTERSECT (VALUES 2))