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

import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.metadata.view.ViewsMetadata;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseRandomizedSchema;
import io.netty.handler.codec.http.HttpResponseStatus;

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
        execute("refresh table custom.t");

        execute("select count(*) from custom.t");
        assertThat((Long) response.rows()[0][0]).isEqualTo(4L);

        execute("delete from custom.t where id=1");
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table custom.t");

        execute("select * from custom.t");
        assertThat(response.rowCount()).isEqualTo(3L);

        execute("delete from custom.t");
        assertThat(response.rowCount()).isEqualTo(3L);
        execute("refresh table custom.t");

        execute("select count(*) from custom.t");
        assertThat((Long) response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void testUpdateById() throws Exception {
        execute("create table custom.t (id int, name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into custom.t (id, name) values (?, ?)", new Object[][]{{1, "A"}, {2, "A"}, {3, "A"}, {4, "A"}});
        execute("refresh table custom.t");

        execute("update custom.t set name='B' where id=1");
        execute("refresh table custom.t");

        execute("select * from custom.t order by id");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("1| B\n2| A\n3| A\n4| A\n");
    }

    @Test
    public void testGetCustomSchema() throws Exception {
        execute("create table custom.t (id int) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into custom.t (id) values (?)", new Object[][]{{1}, {2}, {3}, {4}});
        execute("refresh table custom.t");

        execute("select id from custom.t where id=1");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("1\n");

        execute("select id from custom.t where id in (2,4) order by id");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("2\n4\n");
    }

    @Test
    public void testSelectFromDroppedTableWithMoreThanOneTableInSchema() throws Exception {
        execute("create table custom.foo (id integer)");
        execute("create table custom.bar (id integer)");

        String indexUUID = resolveIndex("custom.foo").getUUID();
        assertThat(cluster().clusterService().state().metadata().hasIndex(indexUUID)).isTrue();
        execute("drop table custom.foo");
        assertThat(cluster().clusterService().state().metadata().hasIndex(indexUUID)).isFalse();

        assertBusy(() -> {
            try {
                execute("select * from custom.foo");
                fail("should wait for cache invalidation");
            } catch (Exception ignored) {
            }
        });
    }

    @Test
    public void test_create_and_drop_schema_roundtrip() throws Exception {
        execute("create schema foobar");
        execute("create schema foo");
        assertSQLError(() -> execute("create schema foobar"))
            .hasPGError(PGErrorStatus.DUPLICATE_SCHEMA)
            .hasHTTPError(HttpResponseStatus.CONFLICT, 40910)
            .hasMessageContaining("Schema 'foobar' already exists");
        execute("create schema if not exists foobar");

        execute("select * from information_schema.schemata order by 1");
        assertThat(response).hasRows(
            "blob",
            "doc",
            "foo",
            "foobar",
            "information_schema",
            "pg_catalog",
            "sys"
        );

        execute("create table foobar.tbl (x int)");
        assertSQLError(() -> execute("drop schema foobar"))
            .hasPGError(PGErrorStatus.DEPENDENT_OBJECTS_STILL_EXIST)
            .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 40013)
            .hasMessageContaining("Cannot drop schema \"foobar\" because other objects depend on it");

        execute("drop table foobar.tbl");

        execute("select * from information_schema.schemata order by 1");
        assertThat(response).hasRows(
            "blob",
            "doc",
            "foo",
            "foobar",
            "information_schema",
            "pg_catalog",
            "sys"
        );

        execute("drop schema foobar, foo");
        execute("select * from information_schema.schemata order by 1");
        assertThat(response).hasRows(
            "blob",
            "doc",
            "information_schema",
            "pg_catalog",
            "sys"
        );
        assertSQLError(() -> execute("drop schema foobar"))
            .hasMessageContaining("Schema 'foobar' unknown")
            .hasHTTPError(HttpResponseStatus.NOT_FOUND, 4045)
            .hasPGError(PGErrorStatus.INVALID_SCHEMA_NAME);
        execute("drop schema if exists foobar");
    }

    @Test
    public void test_drop_schema_with_cascade_deletes_tables() throws Exception {
        execute("create schema foo");
        execute("create table foo.tbl (x int)");
        execute("create view foo.v1 as (select * from foo.tbl)");
        execute("drop schema foo cascade");
        ClusterState state = cluster().getInstance(ClusterService.class).state();
        Metadata metadata = state.metadata();
        assertThat(metadata.indices()).isEmpty();
        assertThat(metadata.schemas()).isEmpty();
        ViewsMetadata viewsMetadata = metadata.custom(ViewsMetadata.TYPE);
        assertThat(viewsMetadata.names()).isEmpty();
    }
}
