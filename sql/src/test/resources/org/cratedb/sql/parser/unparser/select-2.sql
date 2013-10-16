SELECT x,y||y AS yy FROM t WHERE z > 3.14 AND w IS NOT NULL OR y LIKE 'f%' OR y LIKE '_%%' ESCAPE '%'
