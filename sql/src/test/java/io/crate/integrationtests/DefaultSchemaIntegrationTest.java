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

import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.CrateIntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class DefaultSchemaIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private SQLResponse execute(SQLRequest sqlRequest) {
        response = CrateIntegrationTest.client().execute(SQLAction.INSTANCE, sqlRequest).actionGet();
        return response;
    }

    @Test
    public void testSelectFromFooSchemaWithRequestHeaders() throws Exception {
        // this test uses all kind of different statements that involve a table to make sure the schema is applied in each case.

        execute(requestWithSchema("foo", "create table foobar (x string) with (number_of_replicas = 0)"));
        ensureYellow();
        waitNoPendingTasksOnAll();
        execute(requestWithSchema("foo", "alter table foobar set (number_of_replicas = '0-1')"));

        assertThat(getTableCount("foo", "foobar"), is(1L));
        assertThat(getTableCount("doc", "foobar"), is(0L));

        execute(requestWithSchema("foo", "insert into foobar (x) values ('a'), ('b')"));
        execute(requestWithSchema("foo", "refresh table foobar"));
        execute(requestWithSchema("foo", "update foobar set x = 'c'"));
        assertThat(response.rowCount(), is(2L));

        execute(requestWithSchema("foo", "select * from foobar"));
        assertThat(response.rowCount(), is(2L));

        File foobarExport = tmpFolder.newFolder("foobar_export");
        execute(requestWithSchema("foo", "copy foobar to directory ?", foobarExport.getAbsolutePath()));
        execute(requestWithSchema("foo", "delete from foobar"));

        execute(requestWithSchema("foo", "select * from foobar"));
        assertThat(response.rowCount(), is(0L));

        execute(requestWithSchema("foo", "copy foobar from ? with (shared=True)", foobarExport.getAbsolutePath() + "/*"));
        execute(requestWithSchema("foo", "refresh table foobar"));
        execute(requestWithSchema("foo", "select * from foobar"));
        assertThat(response.rowCount(), is(2L));

        execute(requestWithSchema("foo", "insert into foobar (x) (select x from foobar)"));
        execute(requestWithSchema("foo", "refresh table foobar"));
        execute(requestWithSchema("foo", "select * from foobar"));
        assertThat(response.rowCount(), is(4L));

        execute(requestWithSchema("foo", "drop table foobar"));
        assertThat(getTableCount("foo", "foobar"), is(0L));
    }

    private long getTableCount(String schema, String tableName) {
        execute("select count(*) from information_schema.tables where schema_name = ? and table_name = ?",
                new Object[]{schema, tableName});
        return ((long) response.rows()[0][0]);
    }

    private SQLRequest requestWithSchema(String schema, String stmt, Object... args) {
        SQLRequest sqlRequest = new SQLRequest(stmt, args);
        sqlRequest.setDefaultSchema(schema);
        return sqlRequest;
    }

    private SQLRequest requestWithSchema(String schema, String stmt) {
        return requestWithSchema(schema, stmt, new Object[0]);
    }
}
