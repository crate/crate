CREATE TABLE P(id INT PRIMARY KEY, name VARCHAR(10) NOT NULL);
CREATE TABLE C(id INT PRIMARY KEY, name VARCHAR(10) NOT NULL, pid INT GROUPING REFERENCES P(id));
CREATE TABLE G(id INT PRIMARY KEY, value DECIMAL(10,2), cid INT, GROUPING FOREIGN KEY(cid) REFERENCES C(id));
