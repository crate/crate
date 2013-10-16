insert into t (id, name, last_value) values (NEXT VALUE FOR sequence_1, 'fred', 'last')
returning id+1000, name || last_value