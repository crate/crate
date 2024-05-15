/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.testing.Asserts;
import io.crate.testing.TestingHelpers;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class OptimizeTableIntegrationTest extends SQLHttpIntegrationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testOptimize() throws Exception {
        execute("create table test (id int primary key, name string)");
        ensureYellow();
        execute("insert into test (id, name) values (0, 'Trillian'), (1, 'Ford'), (2, 'Zaphod')");
        execute("select count(*) from test");
        assertThat((long) response.rows()[0][0], lessThanOrEqualTo(3L));

        execute("delete from test where id=1");
        refresh();
        execute("optimize table test");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows().length).isEqualTo(0);

        execute("select count(*) from test");
        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    public void testOptimizeBlobTables() throws Exception {
        execute("create blob table blobs with (number_of_replicas = 0)");
        ensureYellow();

        for (String content : new String[]{"bar", "foo", "buzz", "crateDB"}) {
            upload("blobs", content);
        }
        refresh();

        execute("optimize table blob.blobs");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows().length).isEqualTo(0);

        execute("select digest from blob.blobs");
        assertThat(TestingHelpers.printedTable(response.rows()),
            allOf(
                containsString("626f48d2188e903dc1f373f34eebd063b7ca9ff8\n"),
                containsString("62cdb7020ff920e5aa642c3d4066950dd1f01f4d\n"),
                containsString("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n"),
                containsString("6bb2f7cc9eae6a77bcb13cac64098b5fd2b6964b\n")));

    }

    @Test
    public void testOptimizeWithParams() throws Exception {
        execute("create table test (id int primary key, name string)");
        ensureYellow();
        execute("insert into test (id, name) values (0, 'Trillian'), (1, 'Ford'), (2, 'Zaphod')");
        execute("select count(*) from test");
        assertThat((long) response.rows()[0][0], lessThanOrEqualTo(3L));

        execute("delete from test where id=1");
        refresh();
        execute("optimize table test with (max_num_segments=1, only_expunge_deletes=true)");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows().length).isEqualTo(0);

        execute("select count(*) from test");
        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    public void testOptimizeEmptyPartitionedTable() {
        execute(
            "create table parted (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone" +
            ") partitioned by (date) with (refresh_interval=0)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("optimize table parted partition(date=0)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(NOT_FOUND, 4046)
            .hasMessageContaining(String.format("No partition for table '%s' with ident '04130' exists", getFqn("parted")));
    }

    @Test
    public void testOptimizePartitionedTableAllPartitions() {
        execute(
            "create table parted (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone" +
            ") partitioned by (date)");
        ensureYellow();

        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01'), " +
                "(2, 'Groucho', '1970-01-01'), " +
                "(3, 'Harpo', '1970-01-07'), " +
                "(4, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount()).isEqualTo(4L);
        refresh();

        execute("select count(*) from parted");
        assertThat(response.rows()[0][0]).isEqualTo(4L);

        execute("delete from parted where id in (1, 4)");
        refresh();
        execute("optimize table parted");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows().length).isEqualTo(0);

        // assert that all data is available after optimize
        execute("select count(*) from parted");
        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    public void testOptimizePartitionedTableSinglePartitions() {
        execute(
            "create table parted (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone" +
            ") partitioned by (date) with (number_of_replicas=0, refresh_interval=-1)");
        ensureYellow();
        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01')," +
                "(2, 'Groucho', '1970-01-01')," +
                "(3, 'Harpo', '1970-01-05')," +
                "(4, 'Zeppo', '1970-01-05')," +
                "(5, 'Chico', '1970-01-07')," +
                "(6, 'Arthur', '1970-01-08')");
        assertThat(response.rowCount()).isEqualTo(6L);
        refresh();

        // assert that after refresh all rows are available
        execute("select * from parted");
        assertThat(response.rowCount()).isEqualTo(6L);

        execute("delete from parted where id=3");
        refresh();
        execute("optimize table parted PARTITION (date='1970-01-01')");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows().length).isEqualTo(0);

        // assert all partition rows are available after optimize
        execute("select * from parted where date='1970-01-01'");
        assertThat(response.rowCount()).isEqualTo(2L);

        execute("delete from parted where id=4");
        refresh();
        execute("optimize table parted PARTITION (date='1970-01-07')");
        assertThat(response.rowCount()).isEqualTo(1L);

        // assert all partition rows are available after optimize
        execute("select * from parted where date='1970-01-07'");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testOptimizeMultipleTablesWithPartition() {
        execute(
            "create table t1 (" +
            "   id integer, " +
            "   name string, " +
            "   age integer, " +
            "   date timestamp with time zone" +
            ") partitioned by (date, age) " +
            "with (number_of_replicas=0, refresh_interval=-1)");
        ensureYellow();

        execute("insert into t1 (id, name, age, date) values " +
                "(1, 'Trillian', 90, '1970-01-01')," +
                "(2, 'Marvin', 50, '1970-01-07')," +
                "(3, 'Arthur', 50, '1970-01-07')," +
                "(4, 'Zaphod', 90, '1970-01-01')");
        assertThat(response.rowCount()).isEqualTo(4L);

        refresh();
        execute("select * from t1");
        assertThat(response.rowCount()).isEqualTo(4L);

        execute("delete from t1 where id in (1, 2)");
        refresh();
        execute("optimize table t1 partition (age=50, date='1970-01-07')," +
                "               t1 partition (age=90, date='1970-01-01')");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows().length).isEqualTo(0);

        execute("select * from t1");
        assertThat(response.rowCount()).isEqualTo(2L);
    }
}
