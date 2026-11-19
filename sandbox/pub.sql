CREATE USER repl WITH (password = 'repl');
GRANT AL TO repl;

CREATE TABLE IF NOT EXISTS doc.my_table (
    id INT,
    partition_id INT,
    description TEXT,
    PRIMARY KEY (id, partition_id)
) PARTITIONED BY (partition_id);

GRANT DQL ON TABLE doc.my_table TO repl;

CREATE PUBLICATION pub FOR TABLE doc.my_table;

INSERT INTO doc.my_table (id, partition_id, description)
VALUES (1, 1, 'one'), (2, 1, 'two'), (3, 2, 'three');
REFRESH TABLE doc.my_table;
