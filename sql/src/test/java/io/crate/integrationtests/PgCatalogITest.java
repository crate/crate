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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

public class PgCatalogITest extends SQLTransportIntegrationTest {

    @Before
    public void createRelations() {
        execute("create table doc.t1 (id int primary key, s string)");
        execute("create view doc.v1 as select id from doc.t1");
    }

    @After
    public void dropView() {
        execute("drop view doc.v1");
    }

    @Test
    public void testPgClassTable() {
        execute("select * from pg_catalog.pg_class where relname in ('t1', 'v1', 'tables', 'nodes') order by relname");
        assertThat(printedTable(response.rows()), is(
            "-1420189195| NULL| 0| 0| 0| 0| 0| false| false| true| false| false| false| true| false| r| 0| nodes| -458336339| 17| 0| NULL| 0| 0| p| 0| 0| 0| 0.0| 0\n" +
            "728874843| NULL| 0| 0| 0| 0| 0| false| false| true| false| false| false| true| false| r| 0| t1| -2048275947| 2| 0| NULL| 0| 0| p| 0| 0| 0| 0.0| 0\n" +
            "-1689918046| NULL| 0| 0| 0| 0| 0| false| false| true| false| false| false| true| false| r| 0| tables| 204690627| 16| 0| NULL| 0| 0| p| 0| 0| 0| 0.0| 0\n" +
            "845171032| NULL| 0| 0| 0| 0| 0| false| false| false| false| false| false| true| false| v| 0| v1| -2048275947| 1| 0| NULL| 0| 0| p| 0| 0| 0| 0.0| 0\n"));
    }

    @Test
    public void testPgNamespaceTable() {
        execute("select * from pg_catalog.pg_namespace order by nspname");
        assertThat(printedTable(response.rows()), is(
            "NULL| blob| 0| -508866815\n" +
            "NULL| doc| 0| -2048275947\n" +
            "NULL| information_schema| 0| 204690627\n" +
            "NULL| pg_catalog| 0| -68025646\n" +
            "NULL| sys| 0| -458336339\n"));
    }
}
