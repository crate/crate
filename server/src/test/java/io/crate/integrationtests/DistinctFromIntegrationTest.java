/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS"BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class DistinctFromIntegrationTest extends IntegTestCase {

    @Test
    public void testIsDistinctFrom1() {
        execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                manufacturer TEXT DEFAULT NULL
            )""");
        ensureYellow();
        execute("INSERT INTO products (id, name, manufacturer) VALUES (1, 'Laptop', 'TechCorp')");
        execute("INSERT INTO products (id, name, manufacturer) VALUES (2, 'Mobile', NULL)");
        execute("INSERT INTO products (id, name) VALUES (3, 'Tablet')");
        execute("INSERT INTO products (id, name, manufacturer) VALUES (4, 'Monitor', 'TechCorp')");
        execute("INSERT INTO products (id, name, manufacturer) VALUES (5, 'Mobile', 'GadgetInc')");
        execute("refresh table products");

        execute("SELECT * FROM products");
        assertThat(response).hasRowCount(5);
        execute("SELECT * FROM products WHERE manufacturer <> 'TechCorp'");
        assertThat(response).hasRowsInAnyOrder(new Object[]{5, "Mobile", "GadgetInc"});
        execute("SELECT * FROM products WHERE manufacturer IS DISTINCT FROM 'TechCorp' order by id");
        assertThat(response).hasRowsInAnyOrder(new Object[]{2, "Mobile", null}, new Object[]{3, "Tablet", null}, new Object[]{5, "Mobile", "GadgetInc"});
        execute("SELECT * FROM products WHERE manufacturer IS NOT DISTINCT FROM 'TechCorp' order by id");
        assertThat(response).hasRowsInAnyOrder(new Object[]{1, "Laptop", "TechCorp"}, new Object[]{4, "Monitor", "TechCorp"});
        execute("SELECT * FROM products WHERE manufacturer IS DISTINCT FROM null order by id");
        assertThat(response).hasRowsInAnyOrder(new Object[]{1, "Laptop", "TechCorp"}, new Object[]{4, "Monitor", "TechCorp"}, new Object[]{5, "Mobile", "GadgetInc"});
    }

    @Test
    public void testDistinctFromNull() {
        execute("create table custom.t (id int, name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into custom.t (id, name) values (?, ?)", new Object[][]{{1, null}, {2, "A"}, {3, "B"}, {4, "C"}});
        execute("refresh table custom.t");

        execute("select * from custom.t where name <> 'A'");
        assertThat(response).hasRowsInAnyOrder(new Object[]{3, "B"}, new Object[]{4, "C"});

        execute("select * from custom.t where name <> 'A' OR name IS null");
        assertThat(response).hasRowsInAnyOrder(new Object[]{1, null}, new Object[]{3, "B"}, new Object[]{4, "C"});
        execute("select * from custom.t where name is distinct from 'A'");
        assertThat(response).hasRowsInAnyOrder(new Object[]{1, null}, new Object[]{3, "B"}, new Object[]{4, "C"});
        execute("select * from custom.t where name is NOT distinct from 'A'");
        assertThat(response).hasRowsInAnyOrder(new Object[]{2, "A"});

        execute("select * from custom.t where name is distinct from null");
        assertThat(response).hasRowsInAnyOrder(new Object[]{2, "A"}, new Object[]{3, "B"}, new Object[]{4, "C"});
        execute("select * from custom.t where name is NOT distinct from null");
        assertThat(response).hasRowsInAnyOrder(new Object[]{1, null});
    }
}
