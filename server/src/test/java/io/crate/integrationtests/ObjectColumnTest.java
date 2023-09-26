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
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_COLUMN;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.crate.testing.UseNewCluster;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.Asserts;
import io.crate.testing.UseJdbc;

public class ObjectColumnTest extends IntegTestCase {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testInsertIntoDynamicObject() throws Exception {
        this.setup.setUpObjectTable();
        Map<String, Object> authorMap = Map.of(
            "name", Map.of(
                    "first_name", "Douglas",
                    "last_name", "Adams"),
            "age", 49);
        execute("insert into ot (title, author) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                authorMap
            });
        refresh();
        execute("select title, author from ot order by title");
        assertThat(response.rowCount()).isEqualTo(2);
        assertThat(response.rows()[0][0]).isEqualTo("Life, the Universe and Everything");
        assertThat(response.rows()[0][1]).isEqualTo(authorMap);
    }

    @Test
    public void testAddColumnToDynamicObject() throws Exception {
        this.setup.setUpObjectTable();
        Map<String, Object> authorMap = new HashMap<>() {{
                put("name", new HashMap<String, Object>() {{
                        put("first_name", "Douglas");
                        put("last_name", "Adams");
                    }}
                );
                put("dead", true);
                put("age", 49);
            }};
        execute("insert into ot (title, author) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                authorMap
            });
        refresh();
        waitForMappingUpdateOnAll("ot", "author.dead");
        execute("select title, author, author['dead'] from ot order by title");
        assertThat(response.rowCount()).isEqualTo(2);
        assertThat(response.rows()[0][0]).isEqualTo("Life, the Universe and Everything");
        assertThat(response.rows()[0][1]).isEqualTo(authorMap);
        assertThat(response.rows()[0][2]).isEqualTo(true);
    }

    @Test
    @UseJdbc(0) // inserting object requires other treatment for PostgreSQL
    public void testAddColumnToIgnoredObject() throws Exception {
        this.setup.setUpObjectTable();
        Map<String, Object> detailMap = new HashMap<>();
        detailMap.put("num_pages", 240);
        detailMap.put("publishing_date", "1982-01-01");
        detailMap.put("isbn", "978-0345391827");
        detailMap.put("weight", 4.8d);
        execute("insert into ot (title, details) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                detailMap
            });
        refresh();
        execute("select title, details, details['weight'], details['publishing_date'] from ot order by title");
        assertThat(response.rowCount()).isEqualTo(2);
        assertThat(response.rows()[0][0]).isEqualTo("Life, the Universe and Everything");
        assertThat(response.rows()[0][1]).isEqualTo(detailMap);
        assertThat(response.rows()[0][2]).isEqualTo(4.8d);
        assertThat(response.rows()[0][3]).isEqualTo("1982-01-01");
    }

    @Test
    @UseJdbc(0) // inserting object requires other treatment for PostgreSQL
    public void test_predicate_on_ignored_object_returns_zero_rows_after_delete() throws Exception {
        this.setup.setUpObjectTable();
        Map<String, Object> detailMap = new HashMap<>();
        detailMap.put("num_pages", 240);
        detailMap.put("isbn", "978-0345391827");

        execute("insert into ot (details) values (?)",
            new Object[]{
                detailMap
            });
        refresh();
        execute("select * from ot where details['isbn'] = '978-0345391827'");
        assertThat(response.rowCount()).isEqualTo(1);

        // Check to get zero rows after deletion
        // and following access with filter on non-indexed, dynamic field of the ignored object.
        // See https://github.com/crate/crate/issues/11600
        execute("delete from ot");
        execute("refresh table ot");
        // num_pages is indexed as it's specified in Setup.setUpObjectTable, filtering by any other field to verify that
        // SourceParser.parse is null safe.
        execute("select * from ot where details['isbn'] = '978-0345391827'");
        assertThat(response.rowCount()).isEqualTo(0);
    }

    @Test
    public void testAddColumnToStrictObject() throws Exception {
        this.setup.setUpObjectTable();
        Map<String, Object> authorMap = Map.of(
            "name", Map.of(
                "first_name", "Douglas",
                "middle_name", "Noel",
                "last_name", "Adams"),
            "age", 49);
        Asserts.assertSQLError(() -> execute(
            "insert into ot (title, author) values (?, ?)",
            new Object[]{"Life, the Universe and Everything", authorMap}))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot add column `middle_name` to strict object `author['name']`");
    }

    @Test
    public void updateToDynamicObject() throws Exception {
        this.setup.setUpObjectTable();
        execute("update ot set author['job']='Writer' " +
                "where author['name']['first_name']='Douglas' and author['name']['last_name']='Adams'");
        refresh();
        waitForMappingUpdateOnAll("ot", "author.job");
        execute("select author, author['job'] from ot " +
                "where author['name']['first_name']='Douglas' and author['name']['last_name']='Adams'");
        assertThat(response.rowCount()).isEqualTo(1);
        assertEquals(
            Map.of(
                "name", Map.of(
                    "first_name", "Douglas",
                    "last_name", "Adams"),
                "age", 49,
                "job", "Writer"),
            response.rows()[0][0]
        );
        assertThat(response.rows()[0][1]).isEqualTo("Writer");
    }

    @Test
    public void updateToIgnoredObject() throws Exception {
        this.setup.setUpObjectTable();
        execute("update ot set details['published']='1978-01-01' " +
                "where title=?", new Object[]{"The Hitchhiker's Guide to the Galaxy"});
        refresh();
        execute("select details, details['published'] from ot where title=?",
            new Object[]{"The Hitchhiker's Guide to the Galaxy"});
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(
            Map.of(
                "num_pages", 224,
                "published", "1978-01-01"
            ));
    }

    @Test
    public void updateToStrictObject() throws Exception {
        this.setup.setUpObjectTable();
        Asserts.assertSQLError(() -> execute(
            "update ot set author['name']['middle_name']='Noel' where author['name']['first_name']='Douglas' " +
            "and author['name']['last_name']='Adams'"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column author['name']['middle_name'] unknown");
    }

    @Test
    public void selectDynamicAddedColumnWhere() throws Exception {
        this.setup.setUpObjectTable();
        Map<String, Object> authorMap = Map.of(
            "name", Map.of(
                "first_name", "Douglas",
                "last_name", "Adams"),
            "dead", true,
            "age", 49);
        execute("insert into ot (title, author) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                authorMap
            });
        refresh();
        waitForMappingUpdateOnAll("ot", "author.dead");
        execute("select author from ot where author['dead']=true");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(authorMap);
    }

    @Test
    public void selectIgnoredAddedColumnWhere() throws Exception {
        this.setup.setUpObjectTable();
        Map<String, Object> detailMap = Map.of(
            "num_pages", 240,
            "publishing_date", "1982-01-01",
            "isbn", "978-0345391827",
            "weight", 4.8d);
        execute("insert into ot (title, details) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                detailMap
            });
        refresh();
        execute("select details from ot where details['isbn']='978-0345391827'");
        assertThat(response.rowCount()).isEqualTo(1);

        execute("select details from ot where details['num_pages']>224");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(detailMap);
    }

    @Test
    public void selectDynamicAddedColumnOrderBy() throws Exception {
        this.setup.setUpObjectTable();
        Map<String, Object> authorMap = Map.of(
            "name", Map.of(
                "first_name", "Douglas",
                "last_name", "Adams"),
            "dead", true,
            "age", 49);
        execute("insert into ot (title, author) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                authorMap
            }
        );
        execute("insert into ot (title, author) values (?, ?)",
            new Object[]{
                "Don't Panic: Douglas Adams and the \"Hitchhiker's Guide to the Galaxy\"",
                Map.of(
                "name", Map.of(
                        "first_name", "Neil",
                        "last_name", "Gaiman"),
                    "dead", false,
                    "age", 53)
            }
        );
        refresh();
        waitForMappingUpdateOnAll("ot", "author.dead");
        execute("select title, author['dead'] from ot order by author['dead'] desc");
        assertThat(response.rowCount()).isEqualTo(3);
        assertThat(response.rows()[0][0]).isEqualTo("The Hitchhiker's Guide to the Galaxy");
        assertThat(response.rows()[0][1]).isNull();

        assertThat(response.rows()[1][0]).isEqualTo("Life, the Universe and Everything");
        assertThat(response.rows()[1][1]).isEqualTo(true);

        assertThat(response.rows()[2][0]).isEqualTo("Don't Panic: Douglas Adams and the \"Hitchhiker's Guide to the Galaxy\"");
        assertThat(response.rows()[2][1]).isEqualTo(false);
    }

    @Test
    public void testSelectDynamicObjectNewColumns() throws Exception {
        execute("create table test (message string, person object(dynamic)) with (number_of_replicas=0)");
        execute("insert into test (message, person) values " +
                "('I''m addicted to kite', {name='Youri', addresses=[{city='Dirksland', country='NL'}]})");
        execute("refresh table test");

        waitForMappingUpdateOnAll("test", "person.name");
        execute("select message, person['name'], person['addresses']['city'] from test " +
                "where person['name'] = 'Youri'");

        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.cols()).containsExactly(
            "message", "person['name']", "person['addresses']['city']");
        assertThat(response.rows()[0]).containsExactly(
            "I'm addicted to kite", "Youri",
            List.of("Dirksland"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSelectObject() throws Exception {
        execute("create table test (a object as (nested integer)) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (a) values (?)", new Object[]{
            Map.of("nested", 2)
        });
        refresh();

        execute("select a from test");
        assertThat(response.cols()).containsExactly("a");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0].length).isEqualTo(1);
        assertThat((Map<String, Object>) response.rows()[0][0])
            .containsExactly(Map.entry("nested", 2));
    }

    @Test
    public void testAddUnderscoreColumnNameToObjectAtInsert() throws Exception {
        execute("create table test (foo object) with (column_policy = 'dynamic')");
        ensureYellow();
        execute("INSERT INTO test (o) (select {\"_w\"= 20})");
        refresh();
        execute("select count(*) from test");
        assertThat(response.rows()[0][0]).isEqualTo(1L);
    }

    @Test
    public void testSelectUnknownObjectColumnPreservesTheUnknownName() throws Exception {
        try (var session = sqlExecutor.newSession()) {
            session.sessionSettings().setErrorOnUnknownObjectKey(false);
            execute("create table t (a object)");
            execute("explain select a['u'] = 123 from t", session);
        }
        // make sure that a['u'] is kept as requested.
        assertThat(printedTable(response.rows())).contains("[(123 = a['u'])]");
    }

    @Test
    public void test_default_columns_in_object_result_in_object_creation_if_missing_from_insert() throws Exception {
        execute("""
            create table tbl (
                id int,
                o object as (
                    key text default 'd'
                ),
                os array(object as (
                    x text default 'd',
                    y text
                )),
                complex object as (
                    os array(object as (
                        key text default 'd'
                    )),
                    x text default 'd'
                )
            )
            """
        );
        execute("insert into tbl (id) values (1)");
        assertThat(response).hasRowCount(1);

        execute("insert into tbl (id, os) values (2, [{}, null, {y=10}, {x=1, y=2}])");
        assertThat(response).hasRowCount(1);

        execute("insert into tbl (id, complex) values (3, {os=[{}, null]})");
        assertThat(response).hasRowCount(1);

        execute("refresh table tbl");
        assertThat(execute("select id, o, os, complex from tbl order by 1 asc")).hasRows(
            "1| {key=d}| NULL| {x=d}",
            "2| {key=d}| [{x=d}, null, {x=d, y=10}, {x=1, y=2}]| {x=d}",
            "3| {key=d}| NULL| {os=[{key=d}, null], x=d}"
        );
    }

    @Test
    public void test_aliased_unknown_object_key() {
        try (var session = sqlExecutor.newSession()) {
            execute("create table test (o object)", session);
            execute("insert into test values({a=1})", session);
            refresh();

            //session.sessionSettings().setErrorOnUnknownObjectKey(true);
            //assertThatThrownBy(() -> execute("select T.o['unknown'] from (select * from test) T", session))
            //    .isExactlyInstanceOf(ColumnUnknownException.class)
            //        .hasMessage("The object `{a=1}` does not contain the key `unknown`");

            session.sessionSettings().setErrorOnUnknownObjectKey(false);
            execute("select T.o['unknown'] from (select * from test) T", session);
            assertThat(response).hasRows("NULL");
        }
    }

    @Test
    @UseNewCluster
    public void test_add_sub_column_with_numeric_name_into_ignored_object() {
        execute("create table t (o object(ignored) as (a int))");

        // Dynamically adding column with name "2" which will be indexed "as is" and could clash with "o.a" column's oid.
        // Ensure that numeric name of the new ignored sub-column is prefixed
        // and doesn't clash with assigned OID of the known column in the source.
        execute("insert into t (o) values(?)", new Object[]{Map.of("a", 1, "2", 2)});
        refresh();
        execute("SELECT _raw FROM t");
        assertThat(response).hasRows(
            "{\"1\":{\"2\":1,\"_u_2\":2}}"
        );

        execute("SELECT * FROM t");
        assertThat(response).hasRows(
            "{2=2, a=1}"
        );
    }
}
