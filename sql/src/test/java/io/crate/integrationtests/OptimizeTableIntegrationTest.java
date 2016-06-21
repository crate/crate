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

import io.crate.action.sql.SQLActionException;
import io.crate.executor.TaskResult;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class OptimizeTableIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        execute("optimize table test");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows(), is(TaskResult.EMPTY_OBJS));

        execute("select count(*) from test");
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testOptimizeWithParams() throws Exception {
        execute("create table test (id int primary key, name string)");
        ensureYellow();
        execute("insert into test (id, name) values (0, 'Trillian'), (1, 'Ford'), (2, 'Zaphod')");
        execute("select count(*) from test");
        assertThat((long) response.rows()[0][0], lessThanOrEqualTo(3L));

        execute("delete from test where id=1");
        execute("optimize table test with (max_num_segments=1, only_expunge_deletes=true)");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows(), is(TaskResult.EMPTY_OBJS));

        execute("select count(*) from test");
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testOptimizeEmptyPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) " +
                "partitioned by (date) with (refresh_interval=0)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("No partition for table 'doc.parted' with ident '04130' exists");
        execute("optimize table parted partition(date=0)");
    }

    @Test
    public void testOptimizePartitionedTableAllPartitions() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) with (refresh_interval=0)");
        ensureYellow();

        execute("refresh table parted");
        assertThat(response.rowCount(), is(-1L));

        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01'), " +
                "(2, 'Groucho', '1970-01-01'), " +
                "(3, 'Harpo', '1970-01-07'), " +
                "(4, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount(), is(4L));
        ensureYellow();

        execute("refresh table parted");
        execute("select count(*) from parted");
        assertThat((Long) response.rows()[0][0], is(4L));

        execute("delete from parted where id in (1, 4)");
        execute("optimize table parted");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows(), is(TaskResult.EMPTY_OBJS));

        // assert that all is available after optimize
        execute("select count(*) from parted");
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testOptimizePartitionedTableSinglePartitions() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) " +
                "with (number_of_replicas=0, refresh_interval=-1)");
        ensureYellow();
        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01')," +
                "(2, 'Groucho', '1970-01-01')," +
                "(3, 'Harpo', '1970-01-05')," +
                "(4, 'Zeppo', '1970-01-05')," +
                "(5, 'Chico', '1970-01-07')," +
                "(6, 'Arthur', '1970-01-08')");
        assertThat(response.rowCount(), is(6L));

        ensureYellow();
        execute("refresh table parted");
        assertThat(response.rowCount(), is(4L));

        // assert that after refresh all columns are available
        execute("select * from parted");
        assertThat(response.rowCount(), is(6L));

        execute("delete from parted where id=3");
        execute("optimize table parted PARTITION (date='1970-01-01')");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows(), is(TaskResult.EMPTY_OBJS));

        // assert all partition rows are available after optimize
        execute("select * from parted where date='1970-01-01'");
        assertThat(response.rowCount(), is(2L));

        execute("delete from parted where id=4");
        execute("optimize table parted PARTITION (date='1970-01-07')");
        assertThat(response.rowCount(), is(1L));

        // assert all partition rows are available after optimize
        execute("select * from parted where date='1970-01-07'");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testOptimizeMultipleTablesWithPartition() throws Exception {
        execute("create table t1 (" +
                "  id integer, " +
                "  name string, " +
                "  age integer, " +
                "  date timestamp) partitioned by (date, age) " +
                "  with (number_of_replicas=0, refresh_interval=-1)");
        ensureYellow();

        execute("insert into t1 (id, name, age, date) values " +
                "(1, 'Trillian', 90, '1970-01-01')," +
                "(2, 'Marvin', 50, '1970-01-07')," +
                "(3, 'Arthur', 50, '1970-01-07')," +
                "(4, 'Zaphod', 90, '1970-01-01')");
        assertThat(response.rowCount(), is(4L));

        execute("refresh table t1");
        execute("select * from t1 where age in (50, 90)");
        assertThat(response.rowCount(), is(4L));

        execute("delete from t1 where id in (1, 2)");
        execute("optimize table t1 partition (age=50, date='1970-01-07')," +
                "               t1 partition (age=90, date='1970-01-01')");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows(), is(TaskResult.EMPTY_OBJS));

        execute("select * from t1 where age in (50, 90) and date in ('1970-01-07', '1970-01-01')");
        assertThat(response.rowCount(), is(2L));
    }
}
