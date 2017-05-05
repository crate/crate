/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import io.crate.testing.TestingHelpers;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class RenameTableIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testRenameTable() {
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1), (2)");
        refresh();

        execute("alter table t1 rename to t2");
        assertThat(response.rowCount(), is(-1L));
        ensureYellow();

        execute("select * from t2 order by id");
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\n" +
                                                                    "2\n"));

        execute("select * from information_schema.tables where table_name = 't2'");
        assertThat(response.rowCount(), is(1L));

        execute("select * from information_schema.tables where table_name = 't1'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testRenameTableEnsureOldTableNameCanBeUsed() {
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1), (2)");
        refresh();
        execute("alter table t1 rename to t2");
        ensureYellow();

        // creating a new table using the old name must succeed
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        ensureYellow();
        // also inserting must work (no old blocks traces)
        execute("insert into t1 (id) values (1), (2)");
    }

    @Test
    public void testRenameClosedTable() {
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1), (2)");
        refresh();

        execute("alter table t1 close");

        execute("alter table t1 rename to t2");
        assertThat(response.rowCount(), is(-1L));
        ensureYellow();

        execute("select closed from information_schema.tables where table_name = 't2'");
        assertThat(response.rows()[0][0], is(true));
    }

    @Test
    public void testRenamePartitionedTable() {
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        execute("insert into tp1 (id, id2) values (1, 1), (2, 2)");
        refresh();

        execute("alter table tp1 rename to tp2");
        assertThat(response.rowCount(), is(-1L));
        ensureYellow();

        execute("select id from tp2 order by id2");
        assertThat(TestingHelpers.printedTable(response.rows()), is("1\n" +
                                                                    "2\n"));

        execute("select * from information_schema.tables where table_name = 'tp2'");
        assertThat(response.rowCount(), is(1L));

        execute("select * from information_schema.tables where table_name = 'tp1'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testRenamePartitionedTableEnsureOldTableNameCanBeUsed() {
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        execute("insert into tp1 (id, id2) values (1, 1), (2, 2)");
        refresh();
        execute("alter table tp1 rename to tp2");
        ensureYellow();

        // creating a new table using the old name must succeed
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        ensureYellow();
        // also inserting must work (no old blocks traces)
        execute("insert into tp1 (id, id2) values (1, 1), (2, 2)");
    }

    @Test
    public void testRenamePartitionedTablePartitionStayClosed() {
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        execute("insert into tp1 (id, id2) values (1, 1), (2, 2)");
        refresh();

        execute("alter table tp1 partition (id=1) close");

        execute("alter table tp1 rename to tp2");
        ensureYellow();

        execute("select closed from information_schema.table_partitions where partition_ident = '04132'");
        assertThat(response.rows()[0][0], is(true));
    }
}
