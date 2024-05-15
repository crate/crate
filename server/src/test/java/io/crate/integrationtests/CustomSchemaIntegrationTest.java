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
import static org.junit.jupiter.api.Assertions.fail;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseRandomizedSchema;

public class CustomSchemaIntegrationTest extends IntegTestCase {

    @Test
    @UseRandomizedSchema(random = false)
    public void testInformationSchemaTablesReturnCorrectTablesIfCustomSchemaIsSimilarToTableName() throws Exception {
        // regression test.. this caused foobar to be detected as a table in the foo schema and caused a NPE
        execute("create table foobar (id int primary key) with (number_of_replicas = 0)");
        execute("create table foo.bar (id int primary key) with (number_of_replicas = 0)");

        execute("select table_schema, table_name from information_schema.tables " +
                "where table_name like 'foo%' or table_schema = 'foo' order by table_name");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("" +
                                                                    "foo| bar\n" +
                                                                    "doc| foobar\n");
    }

    @Test
    public void testDeleteFromCustomTable() throws Exception {
        execute("create table custom.t (id int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into custom.t (id) values (?)", new Object[][]{{1}, {2}, {3}, {4}});
        refresh();

        execute("select count(*) from custom.t");
        assertThat((Long) response.rows()[0][0]).isEqualTo(4L);

        execute("delete from custom.t where id=1");
        assertThat(response.rowCount()).isEqualTo(1L);
        refresh();

        execute("select * from custom.t");
        assertThat(response.rowCount()).isEqualTo(3L);

        execute("delete from custom.t");
        assertThat(response.rowCount()).isEqualTo(3L);
        refresh();

        execute("select count(*) from custom.t");
        assertThat((Long) response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void testUpdateById() throws Exception {
        execute("create table custom.t (id int, name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into custom.t (id, name) values (?, ?)", new Object[][]{{1, "A"}, {2, "A"}, {3, "A"}, {4, "A"}});
        refresh();

        execute("update custom.t set name='B' where id=1");
        refresh();

        execute("select * from custom.t order by id");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("1| B\n2| A\n3| A\n4| A\n");
    }

    @Test
    public void testGetCustomSchema() throws Exception {
        execute("create table custom.t (id int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into custom.t (id) values (?)", new Object[][]{{1}, {2}, {3}, {4}});
        refresh();

        execute("select id from custom.t where id=1");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("1\n");

        execute("select id from custom.t where id in (2,4) order by id");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("2\n4\n");
    }

    @Test
    public void testSelectFromDroppedTableWithMoreThanOneTableInSchema() throws Exception {
        execute("create table custom.foo (id integer)");
        execute("create table custom.bar (id integer)");

        assertThat(cluster().clusterService().state().metadata().hasIndex("custom.foo")).isTrue();
        execute("drop table custom.foo");
        assertThat(cluster().clusterService().state().metadata().hasIndex("custom.foo")).isFalse();

        assertBusy(() -> {
            try {
                execute("select * from custom.foo");
                fail("should wait for cache invalidation");
            } catch (Exception ignored) {
            }
        });
    }

}
