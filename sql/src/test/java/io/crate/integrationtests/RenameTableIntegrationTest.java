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
        assertThat(response.rowCount(), is(0L));
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
    public void testRenameTableFailsButOldTableRemainOpen() {
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1), (2)");
        refresh();

        try {
            // will fail due to invalid target table name
            execute("alter table t1 rename to _t2");
        } catch (SQLActionException e) {
            ensureYellow();
            execute("select * from t1 order by id");
            assertThat(TestingHelpers.printedTable(response.rows()), is("1\n" +
                                                                        "2\n"));
        }
    }

    @Test
    public void testRenamePartitionedTable() {
        execute("create table tp1 (id int) partitioned by (id) with (number_of_replicas = 0)");
        execute("insert into tp1 (id) values (1), (2)");
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Renaming a partitioned table is not yet supported");
        execute("alter table tp1 rename to tp2");
    }
}
