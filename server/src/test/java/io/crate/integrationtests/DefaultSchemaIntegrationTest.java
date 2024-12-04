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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Paths;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.session.Session;
import io.crate.testing.SQLResponse;

public class DefaultSchemaIntegrationTest extends IntegTestCase {


    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Override
    public SQLResponse execute(String stmt, Object[] args, String schema) {
        try (Session session = createSession(schema)) {
            return execute(stmt, args, session);
        }
    }

    @Test
    public void testSelectFromFooSchemaWithRequestHeaders() throws Exception {
        // this test uses all kind of different statements that involve a table to make sure the schema is applied in each case.

        execute("create table foobar (x string) with (number_of_replicas = 0)", "foo");
        ensureYellow();
        waitNoPendingTasksOnAll();
        execute("alter table foobar set (number_of_replicas = '0-1')", "foo");

        assertThat(getTableCount("foo", "foobar")).isEqualTo(1L);
        assertThat(getTableCount("doc", "foobar")).isEqualTo(0L);

        execute("insert into foobar (x) values ('a'), ('b')", "foo");
        execute("refresh table foobar", "foo");
        execute("update foobar set x = 'c'", "foo");
        assertThat(response.rowCount()).isEqualTo(2L);

        execute("select * from foobar", "foo");
        assertThat(response.rowCount()).isEqualTo(2L);

        File foobarExport = tmpFolder.newFolder("foobar_export");
        String uriTemplate = Paths.get(foobarExport.toURI()).toUri().toString();
        execute("copy foobar to directory ?", new Object[]{uriTemplate}, "foo");
        execute("delete from foobar", "foo");
        execute("refresh table foobar", "foo");

        execute("select * from foobar", "foo");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("copy foobar from ? with (shared=True)", new Object[]{uriTemplate + "*"}, "foo");
        execute("refresh table foobar", "foo");
        execute("select * from foobar", "foo");
        assertThat(response.rowCount()).isEqualTo(2L);

        execute("insert into foobar (x) (select x from foobar)", "foo");
        execute("refresh table foobar", "foo");
        execute("select * from foobar", "foo");
        assertThat(response.rowCount()).isEqualTo(4L);

        execute("drop table foobar", "foo");
        assertThat(getTableCount("foo", "foobar")).isEqualTo(0L);
    }

    private long getTableCount(String schema, String tableName) {
        execute("select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
            new Object[]{schema, tableName});
        return ((long) response.rows()[0][0]);
    }

}
