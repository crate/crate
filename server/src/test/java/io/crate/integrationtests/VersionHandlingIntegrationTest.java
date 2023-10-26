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
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.io.IOException;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.exceptions.VersioningValidationException;
import io.crate.testing.Asserts;
import io.crate.testing.UseRandomizedOptimizerRules;

public class VersionHandlingIntegrationTest extends IntegTestCase {

    private final Setup setup = new Setup(sqlExecutor);

    @Test
    public void selectMultiGetRequestWithColumnAlias() throws IOException {
        this.setup.createTestTableWithPrimaryKey();
        execute("insert into test (pk_col, message) values ('1', 'foo')");
        execute("insert into test (pk_col, message) values ('2', 'bar')");
        execute("insert into test (pk_col, message) values ('3', 'baz')");
        refresh();
        execute("SELECT pk_col as id, message from test where pk_col IN (?,?)", new Object[]{"1", "2"});
        assertThat(response).hasRowCount(2L);
        assertThat(response.cols()).containsExactlyInAnyOrder("id", "message");
        assertThat(new String[]{(String) response.rows()[0][0], (String) response.rows()[1][0]})
                .containsExactlyInAnyOrder("1", "2");
    }

    @Test
    public void testDeleteWhereVersion() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertThat(response).hasRows("1");
        Long version = (Long) response.rows()[0][0];

        execute("delete from test where col1 = 1 and \"_version\" = ?",
            new Object[]{version});
        assertThat(response).hasRowCount(1L);

        // Validate that the row is really deleted
        refresh();
        execute("select * from test where col1 = 1");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testDeleteWhereVersionWithConflict() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertThat(response).hasRows("1");

        execute("update test set col2 = ? where col1 = ?", new Object[]{"ok now panic", 1});
        assertThat(response).hasRowCount(1L);
        refresh();

        execute("delete from test where col1 = 1 and \"_version\" = 1");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testUpdateWhereVersionWithPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertThat(response).hasRows("1");

        execute("update test set col2 = ? where col1 = ? and \"_version\" = ?",
            new Object[]{"ok now panic", 1, 1});
        assertThat(response).hasRowCount(1L);

        // Validate that the row is really updated
        refresh();
        execute("select col2 from test where col1 = 1");
        assertThat(response).hasRows("ok now panic");
    }

    @Test
    public void testUpdateWhereVersionWithoutPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();
        Asserts.assertSQLError(() -> execute("update test set col2 = ? where \"_version\" = ?", new Object[]{"ok now panic", 1}))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    public void testUpdateWhereVersionWithConflict() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();

        execute("insert into test (col1, col2) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select \"_version\" from test where col1 = 1");
        assertThat(response).hasRows("1");

        execute("update test set col2 = ? where col1 = ? and \"_version\" = ?",
            new Object[]{"ok now panic", 1, 1});
        assertThat(response).hasRowCount(1L);
        refresh();

        execute("update test set col2 = ? where col1 = ? and \"_version\" = ?",
            new Object[]{"hopefully not updated", 1, 1});
        assertThat(response).hasRowCount(0L);
        refresh();

        // Validate that the row is really NOT updated
        refresh();
        execute("select col2 from test where col1 = 1");
        assertThat(response).hasRows("ok now panic");
    }

    @Test
    public void testSelectWhereVersionWithoutPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();
        Asserts.assertSQLError(() -> execute("select _version from test where col2 = 'hello' and _version = 1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining(VersioningValidationException.VERSION_COLUMN_USAGE_MSG);
    }

    @Test
    @UseRandomizedOptimizerRules(0) // depends on primary key lookup for _version query
    public void testSelectWhereVersionWithPrimaryKey() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        execute("insert into test (col1, col2) values (1, 'foo')");
        execute("select _version from test where col1 = 1 and _version = 1");
        assertThat(response).hasRows("1");
    }

    @Test
    @UseRandomizedOptimizerRules(0) // depends on primary key lookup for _version query
    public void testSelectGroupByVersion() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        execute("insert into test (col1, col2) values (1, 'bar'), (2, 'bar')");
        execute("refresh table test");
        execute("select col2, count(*) from test where col1 = 1 and _version = 1 group by col2");
        assertThat(response).hasRows("bar| 1");
    }
}
