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

import io.crate.testing.UseJdbc;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_COLUMN;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ObjectColumnTest extends SQLIntegrationTestCase {

    private Setup setup = new Setup(sqlExecutor);

    @Before
    public void initTestData() {
        this.setup.setUpObjectTable();
        ensureYellow();
    }

    @Test
    public void testInsertIntoDynamicObject() throws Exception {
        Map<String, Object> authorMap = new HashMap<String, Object>() {{
            put("name", new HashMap<String, Object>() {{
                put("first_name", "Douglas");
                put("last_name", "Adams");
            }});
            put("age", 49);
        }};
        execute("insert into ot (title, author) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                authorMap
            });
        refresh();
        execute("select title, author from ot order by title");
        assertEquals(2, response.rowCount());
        assertEquals("Life, the Universe and Everything", response.rows()[0][0]);
        assertEquals(authorMap, response.rows()[0][1]);
    }

    @Test
    public void testAddColumnToDynamicObject() throws Exception {
        Map<String, Object> authorMap = new HashMap<String, Object>() {{
            put("name", new HashMap<String, Object>() {{
                put("first_name", "Douglas");
                put("last_name", "Adams");
            }});
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
        assertEquals(2, response.rowCount());
        assertEquals("Life, the Universe and Everything", response.rows()[0][0]);
        assertEquals(authorMap, response.rows()[0][1]);
        assertEquals(true, response.rows()[0][2]);
    }

    @Test
    @UseJdbc(0) // inserting object requires other treatment for PostgreSQL
    public void testAddColumnToIgnoredObject() throws Exception {
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
        assertEquals(2, response.rowCount());
        assertEquals("Life, the Universe and Everything", response.rows()[0][0]);
        assertEquals(detailMap, response.rows()[0][1]);
        assertEquals(4.8d, response.rows()[0][2]);
        assertEquals("1982-01-01", response.rows()[0][3]);
    }

    @Test
    public void testAddColumnToStrictObject() throws Exception {
        Map<String, Object> authorMap = Map.of(
            "name", Map.of(
                "first_name", "Douglas",
                "middle_name", "Noel",
                "last_name", "Adams"),
            "age", 49);
        assertThrows(() -> execute(
            "insert into ot (title, author) values (?, ?)",
            new Object[]{"Life, the Universe and Everything", authorMap}),
                     isSQLError(containsString("dynamic introduction of [middle_name] within [author.name] is not allowed"),
                                INTERNAL_ERROR, BAD_REQUEST, 4000));
    }

    @Test
    public void updateToDynamicObject() throws Exception {
        execute("update ot set author['job']='Writer' " +
                "where author['name']['first_name']='Douglas' and author['name']['last_name']='Adams'");
        refresh();
        waitForMappingUpdateOnAll("ot", "author.job");
        execute("select author, author['job'] from ot " +
                "where author['name']['first_name']='Douglas' and author['name']['last_name']='Adams'");
        assertEquals(1, response.rowCount());
        assertEquals(
            new HashMap<String, Object>() {{
                put("name", new HashMap<String, Object>() {{
                    put("first_name", "Douglas");
                    put("last_name", "Adams");
                }});
                put("age", 49);
                put("job", "Writer");
            }},
            response.rows()[0][0]
        );
        assertEquals("Writer", response.rows()[0][1]);
    }

    @Test
    public void updateToIgnoredObject() throws Exception {
        execute("update ot set details['published']='1978-01-01' " +
                "where title=?", new Object[]{"The Hitchhiker's Guide to the Galaxy"});
        refresh();
        execute("select details, details['published'] from ot where title=?",
            new Object[]{"The Hitchhiker's Guide to the Galaxy"});
        assertThat(response.rowCount(), is(1L));
        assertThat(
            response.rows()[0][0],
            is(Map.of(
                "num_pages", 224,
                "published", "1978-01-01"
            ))
        );
    }

    @Test
    public void updateToStrictObject() throws Exception {
        assertThrows(() -> execute(
            "update ot set author['name']['middle_name']='Noel' where author['name']['first_name']='Douglas' " +
            "and author['name']['last_name']='Adams'"),
                     isSQLError(is("Column author['name']['middle_name'] unknown"),
                                UNDEFINED_COLUMN,
                                NOT_FOUND,
                                4043));
    }

    @Test
    public void selectDynamicAddedColumnWhere() throws Exception {
        Map<String, Object> authorMap = new HashMap<String, Object>() {{
            put("name", new HashMap<String, Object>() {{
                put("first_name", "Douglas");
                put("last_name", "Adams");
            }});
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
        execute("select author from ot where author['dead']=true");
        assertEquals(1, response.rowCount());
        assertEquals(authorMap, response.rows()[0][0]);
    }

    @Test
    public void selectIgnoredAddedColumnWhere() throws Exception {
        Map<String, Object> detailMap = new HashMap<String, Object>() {{
            put("num_pages", 240);
            put("publishing_date", "1982-01-01");
            put("isbn", "978-0345391827");
            put("weight", 4.8d);
        }};
        execute("insert into ot (title, details) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                detailMap
            });
        refresh();
        execute("select details from ot where details['isbn']='978-0345391827'");
        assertEquals(1, response.rowCount());

        execute("select details from ot where details['num_pages']>224");
        assertEquals(1, response.rowCount());
        assertEquals(detailMap, response.rows()[0][0]);
    }

    @Test
    public void selectDynamicAddedColumnOrderBy() throws Exception {
        Map<String, Object> authorMap = new HashMap<String, Object>() {{
            put("name", new HashMap<String, Object>() {{
                put("first_name", "Douglas");
                put("last_name", "Adams");
            }});
            put("dead", true);
            put("age", 49);
        }};
        execute("insert into ot (title, author) values (?, ?)",
            new Object[]{
                "Life, the Universe and Everything",
                authorMap
            }
        );
        execute("insert into ot (title, author) values (?, ?)",
            new Object[]{
                "Don't Panic: Douglas Adams and the \"Hitchhiker's Guide to the Galaxy\"",
                new HashMap<String, Object>() {{
                    put("name", new HashMap<String, Object>() {{
                        put("first_name", "Neil");
                        put("last_name", "Gaiman");
                    }});
                    put("dead", false);
                    put("age", 53);
                }}
            }
        );
        refresh();
        waitForMappingUpdateOnAll("ot", "author.dead");
        execute("select title, author['dead'] from ot order by author['dead'] desc");
        assertEquals(3, response.rowCount());
        assertEquals("The Hitchhiker's Guide to the Galaxy", response.rows()[0][0]);
        assertNull(response.rows()[0][1]);

        assertEquals("Life, the Universe and Everything", response.rows()[1][0]);
        assertEquals(true, response.rows()[1][1]);

        assertEquals("Don't Panic: Douglas Adams and the \"Hitchhiker's Guide to the Galaxy\"", response.rows()[2][0]);
        assertEquals(false, response.rows()[2][1]);
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

        assertEquals(1L, response.rowCount());
        assertArrayEquals(new String[]{"message", "person['name']", "person['addresses']['city']"},
            response.cols());
        assertThat(
            response.rows()[0],
            arrayContaining("I'm addicted to kite", "Youri", List.of("Dirksland")));
    }

    @Test
    public void testSelectObject() throws Exception {
        execute("create table test (a object as (nested integer)) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test (a) values (?)", new Object[]{
            Map.of("nested", 2)
        });
        refresh();

        execute("select a from test");
        assertArrayEquals(new String[]{"a"}, response.cols());
        assertEquals(1, response.rowCount());
        assertEquals(1, response.rows()[0].length);
        assertThat((Map<String, Object>) response.rows()[0][0], Matchers.<String, Object>hasEntry("nested", 2));
    }

    @Test
    public void testAddUnderscoreColumnNameToObjectAtInsert() throws Exception {
        execute("create table test (foo object) with (column_policy = 'dynamic')");
        ensureYellow();
        execute("INSERT INTO test (o) (select {\"_w\"= 20})");
        refresh();
        execute("select count(*) from test");
        assertThat(response.rows()[0][0], is(1L));
    }
}
