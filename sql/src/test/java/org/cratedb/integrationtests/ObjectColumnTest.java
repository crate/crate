/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.integrationtests;

import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.sql.ColumnUnknownException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class ObjectColumnTest extends SQLTransportIntegrationTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SQLResponse response;
    private Setup setup = new Setup(this);
    private boolean setUpDone = false;

    @Before
    public void initTestData() {
        if (!setUpDone) {
            this.setup.setUpObjectTable();
            setUpDone = true;
        }
    }

    /**
     * override execute to store response in property for easier access
     */
    @Override
    public SQLResponse execute(String stmt, Object[] args) {
        response = super.execute(stmt, args);
        return response;
    }

    @Test
    public void testInsertIntoDynamicObject() throws Exception {
        Map<String, Object> authorMap = new HashMap<String, Object>(){{
            put("name", new HashMap<String, Object>(){{
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
        Map<String, Object> authorMap = new HashMap<String, Object>(){{
            put("name", new HashMap<String, Object>(){{
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
        execute("select title, author, author['dead'] from ot order by title");
        assertEquals(2, response.rowCount());
        assertEquals("Life, the Universe and Everything", response.rows()[0][0]);
        assertEquals(authorMap, response.rows()[0][1]);
        assertEquals(true, response.rows()[0][2]);
    }

    @Test
    public void testAddColumnToIgnoredObject() throws Exception {
        Map<String, Object> detailMap = new HashMap<String, Object>(){{
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
        execute("select title, details, details['weight'], details['publishing_date'] from ot order by title");
        assertEquals(2, response.rowCount());
        assertEquals("Life, the Universe and Everything", response.rows()[0][0]);
        assertEquals(detailMap, response.rows()[0][1]);
        assertEquals(4.8d, response.rows()[0][2]);
        assertEquals("1982-01-01", response.rows()[0][3]);
    }

    @Test
    public void testAddColumnToStrictObject() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column 'author.name.middle_name' unknown");

        Map<String, Object> authorMap = new HashMap<String, Object>(){{
            put("name", new HashMap<String, Object>(){{
                put("first_name", "Douglas");
                put("middle_name", "Noel");
                put("last_name", "Adams");
            }});
            put("age", 49);
        }};
        execute("insert into ot (title, author) values (?, ?)",
                new Object[]{
                        "Life, the Universe and Everything",
                        authorMap
                });
    }

    @Test
    public void updateToDynamicObject() throws Exception {
        execute("update ot set author['job']='Writer' " +
                "where author['name']['first_name']='Douglas' and author['name']['last_name']='Adams'");
        refresh();
        execute("select author, author['job'] from ot where author['name']['first_name']='Douglas' and author['name']['last_name']='Adams'");
        assertEquals(1, response.rowCount());
        assertEquals(
                new HashMap<String, Object>(){{
                    put("name", new HashMap<String, Object>(){{
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
        assertEquals(1, response.rowCount());
        assertEquals(
                new HashMap<String, Object>(){{
                    put("num_pages", 224);
                    put("published", "1978-01-01");
                }},
                response.rows()[0][0]
        );
        assertEquals("1978-01-01", response.rows()[0][1]);
    }

    @Test
    public void updateToStrictObject() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column 'author.name.middle_name' unknown");

        execute("update ot set author['name']['middle_name']='Noel' " +
                "where author['name']['first_name']='Douglas' and author['name']['last_name']='Adams'");
    }

    @Test
    public void selectDynamicAddedColumnWhere() throws Exception {
        Map<String, Object> authorMap = new HashMap<String, Object>(){{
            put("name", new HashMap<String, Object>(){{
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
        execute("select author from ot where author['dead']=true");
        assertEquals(1, response.rowCount());
        assertEquals(authorMap, response.rows()[0][0]);
    }

    @Test
    public void selectIgnoredAddedColumnWhere() throws Exception {
        Map<String, Object> detailMap = new HashMap<String, Object>(){{
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
        assertEquals(0, response.rowCount());

        execute("select details from ot where details['num_pages']>224");
        assertEquals(1, response.rowCount());
        assertEquals(detailMap, response.rows()[0][0]);
    }

    @Test
    public void selectDynamicAddedColumnOrderBy() throws Exception {
        Map<String, Object> authorMap = new HashMap<String, Object>(){{
            put("name", new HashMap<String, Object>(){{
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
                            put("name", new HashMap<String, Object>(){{
                                put("first_name", "Neil");
                                put("last_name", "Gaiman");
                            }});
                            put("dead", false);
                            put("age", 53);
                        }}
                }
        );
        refresh();
        ensureGreen();
        execute("select title, author['dead'] from ot order by author['dead'] desc");
        assertEquals(3, response.rowCount());
        assertEquals("Life, the Universe and Everything", response.rows()[0][0]);
        assertEquals(true, response.rows()[0][1]);

        assertEquals("Don't Panic: Douglas Adams and the \"Hitchhiker's Guide to the Galaxy\"", response.rows()[1][0]);
        assertEquals(false, response.rows()[1][1]);

        assertEquals("The Hitchhiker's Guide to the Galaxy", response.rows()[2][0]);
        assertNull(response.rows()[2][1]);
    }

    @Test
    public void testSelectIgnoredAddedColumnOrderBy() throws Exception {
        Map<String, Object> detailMap = new HashMap<String, Object>(){{
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
        execute("insert into ot (title, details) values (?, ?)",
                new Object[]{
                        "The Restaurant at the End of the Universe",
                        new HashMap<String, Object>(){{
                            put("num_pages", 256);
                            put("publishing_date", "1980-01-01");
                            put("isbn", "978-0345391810");
                            put("weight", 6.4d);
                        }}
                });
        refresh();
        execute("select title, details['weight'] from ot order by details['weight'] desc, title");
        assertEquals(3, response.rowCount());
        assertEquals("Life, the Universe and Everything", response.rows()[0][0]);
        assertEquals(4.8d, response.rows()[0][1]);
        assertEquals("The Hitchhiker's Guide to the Galaxy", response.rows()[1][0]);
        assertNull(response.rows()[1][1]);
        assertEquals("The Restaurant at the End of the Universe", response.rows()[2][0]);
        assertEquals(6.4d, response.rows()[2][1]);
    }
}
