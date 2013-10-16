insert into t1 (id, name, last_value) values
(select id, name, last_value from t2
order by name)
returning id