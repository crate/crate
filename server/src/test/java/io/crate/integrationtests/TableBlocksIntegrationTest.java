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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.junit.Assert.assertThat;

import java.util.Locale;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.testing.Asserts;

@IntegTestCase.ClusterScope(numDataNodes = 1)
public class TableBlocksIntegrationTest extends IntegTestCase {

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

        Asserts.assertSQLError(() -> execute("insert into t1 (id) values (1)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow INSERT operations",
                sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testUpdateForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.write\" = true, \"blocks.read\" = true)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("update t1 set id = 2"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow UPDATE operations",
                sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testDeleteForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.write\" = true, \"blocks.read\" = true)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("delete from t1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow DELETE operations",
                sqlExecutor.getCurrentSchema()
            ));
    }

    @Test
    public void testDropForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.metadata\" = true)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("drop table t1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow DROP operations",
                sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testAlterAddColumnForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.metadata\" = true)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("alter table t1 add column name string"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow ALTER operations",
                sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testAlterSetForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.metadata\" = true)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("alter table t1 set (number_of_replicas = 1)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow ALTER operations",
                sqlExecutor.getCurrentSchema())
            );
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

        Asserts.assertSQLError(() -> execute("select * from t1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow READ operations",
                sqlExecutor.getCurrentSchema())
            );
    }

    @Test
    public void testNestedReadForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        execute("create table t2 (id integer) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t1 (id) values (1)");

        Asserts.assertSQLError(() -> execute("insert into t2 (id) (select id from t1)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow READ operations",
                sqlExecutor.getCurrentSchema())
            );
    }

    @Test
    public void testReadForbiddenUsingJoins() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        execute("create table t2 (id integer) with (number_of_replicas = 0)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("select * from t2, t1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow READ operations",
                sqlExecutor.getCurrentSchema())
            );
    }

    @Test
    public void testCopyToForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");

        Asserts.assertSQLError(() -> execute("copy t1 to DIRECTORY '/tmp/'"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow COPY TO operations",
                sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testCopyFromForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.write\" = true)");
        ensureYellow();

        Asserts.assertSQLError(() -> execute("copy t1 from '/tmp'"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow INSERT operations",
                sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testCreateSnapshotConcreteTableForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        ensureYellow();
        execute("CREATE REPOSITORY repo TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{TEMPORARY_FOLDER.newFolder().getAbsolutePath()});

        Asserts.assertSQLError(() -> execute("CREATE SNAPSHOT repo.snap TABLE t1 WITH (wait_for_completion=true)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow CREATE SNAPSHOT operations",
                sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testCreateSnapshotAllForbidden() throws Exception {
        execute("create table t1 (id integer) with (number_of_replicas = 0, \"blocks.read\" = true)");
        ensureYellow();
        execute("CREATE REPOSITORY repo TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{TEMPORARY_FOLDER.newFolder().getAbsolutePath()});

        Asserts.assertSQLError(() -> execute("CREATE SNAPSHOT repo.snap ALL WITH (wait_for_completion=true)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow READ operations",
                sqlExecutor.getCurrentSchema()));
    }

    @Test
    public void testInsertIntoPartitionedTableWhileTableReadOnly() {
        execute(
            "create table t1 (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone) " +
            "partitioned by (date) with (number_of_replicas = 0)");
        ensureYellow();
        execute("alter table t1 set (\"blocks.read_only\" = true)");

        execute("select settings['blocks']['read_only'] from information_schema.tables where table_name = 't1'");
        assertThat((Boolean) response.rows()[0][0], Is.is(true));

        Asserts.assertSQLError(() -> execute("insert into t1 (id, name, date) values (?, ?, ?)",
                                   new Object[]{1, "Ford", 13959981214861L}))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4007)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "The relation \"%s.t1\" doesn't support or allow INSERT operations",
                sqlExecutor.getCurrentSchema()));
    }
}
