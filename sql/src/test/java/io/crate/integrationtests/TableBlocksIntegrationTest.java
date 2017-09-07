/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.action.sql.SQLActionException;
import io.crate.testing.UseJdbc;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
@UseJdbc
public class TableBlocksIntegrationTest extends SQLTransportIntegrationTest {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMPORARY_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // we must disable read_only & metadata, otherwise table cannot be dropped
        // only allowed if 1 setting is changed so issue 2 statements
        execute("alter table t1 set (\"blocks.read_only\" = false)");
        execute("alter table t1 set (\"blocks.metadata\" = false)");
        super.tearDown();
    }

    @Test
    public void testInsertForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.write\" = true)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow INSERT operations.");
        execute("insert into t1 (id) values (1)");
    }

    @Test
    public void testUpdateForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.write\" = true, \"blocks.read\" = true)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow UPDATE operations.");
        execute("update t1 set id = 2");
    }

    @Test
    public void testDeleteForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.write\" = true, \"blocks.read\" = true)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow DELETE operations.");
        execute("delete from t1");
    }

    @Test
    public void testDropForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.metadata\" = true)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow DROP operations.");
        execute("drop table t1");
    }

    @Test
    public void testAlterAddColumnForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.metadata\" = true)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow ALTER operations.");
        execute("alter table t1 add column name string");
    }

    @Test
    public void testAlterSetForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.metadata\" = true)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow ALTER operations.");
        execute("alter table t1 set (number_of_replicas = 1)");
    }

    @Test
    public void testAlterSetBlocksAllowed() throws Exception {
        // test changing 1 block setting is allowed even if metadata changes are blocked
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.metadata\" = true)");
        ensureYellow();

        execute("alter table t1 set (\"blocks.metadata\" = true)");
    }

    @Test
    public void testReadForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        ensureYellow();

        execute("select settings['blocks']['read'] from information_schema.tables where table_name = 't1'");
        assertThat((Boolean) response.rows()[0][0], Is.is(true));

        execute("insert into t1 (id) values (1)");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow READ operations.");
        execute("select * from t1");
    }

    @Test
    public void testNestedReadForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        execute("create table t2 (id integer) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t1 (id) values (1)");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow READ operations.");
        execute("insert into t2 (id) (select id from t1)");
    }

    @Test
    public void testReadForbiddenUsingJoins() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        execute("create table t2 (id integer) with (number_of_replicas = 0)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow READ operations.");
        execute("select * from t2, t1");
    }

    @Test
    public void testCopyToForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow COPY TO operations.");
        execute("copy t1 to DIRECTORY '/tmp/'");
    }

    @Test
    public void testCopyFromForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.write\" = true)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow INSERT operations.");
        execute("copy t1 from '/tmp'");
    }

    @Test
    public void testCreateSnapshotConcreteTableForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        ensureYellow();
        execute("CREATE REPOSITORY repo TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{TEMPORARY_FOLDER.newFolder().getAbsolutePath()});

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow CREATE SNAPSHOT " +
                                        "operations.");
        execute("CREATE SNAPSHOT repo.snap TABLE t1 WITH (wait_for_completion=true)");
    }

    @Test
    public void testCreateSnapshotAllForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        ensureYellow();
        execute("CREATE REPOSITORY repo TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{TEMPORARY_FOLDER.newFolder().getAbsolutePath()});

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow READ operations.");
        execute("CREATE SNAPSHOT repo.snap ALL WITH (wait_for_completion=true)");
    }

    @Test
    public void testInsertIntoPartitionedTableWhileTableReadOnly() throws Exception {
        execute("create table t1 (id integer, name string, date timestamp)" +
                "partitioned by (date) with (number_of_replicas = 0)");
        ensureYellow();
        execute("alter table t1 set (\"blocks.read_only\" = true)");

        execute("select settings['blocks']['read_only'] from information_schema.tables where table_name = 't1'");
        assertThat((Boolean) response.rows()[0][0], Is.is(true));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The relation \"doc.t1\" doesn't support or allow INSERT operations, " +
                                        "as it is read-only.");
        execute("insert into t1 (id, name, date) values (?, ?, ?)",
            new Object[]{1, "Ford", 13959981214861L});
    }
}
