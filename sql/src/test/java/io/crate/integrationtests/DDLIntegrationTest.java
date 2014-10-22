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

import io.crate.action.sql.SQLActionException;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class DDLIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();



    @Test
    public void testCreateTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        String expectedMapping = "{\"default\":{" +
                "\"dynamic\":\"true\"," +
                "\"_meta\":{\"primary_keys\":[\"col1\"]}," +
                "\"_all\":{\"enabled\":false}," +
                "\"properties\":{" +
                "\"col1\":{\"type\":\"integer\",\"doc_values\":true}," +
                "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true}" +
                "}}}";

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);

        // test index usage
        execute("insert into test (col1, col2) values (1, 'foo')");
        assertEquals(1, response.rowCount());
        refresh();
        execute("SELECT * FROM test");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testCreateTableWithRefreshIntervalDisableRefresh() throws Exception {
        execute("create table test (id int primary key, content string) with (refresh_interval=0)");
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.refresh_interval\":\"0\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);

        execute("ALTER TABLE test SET (refresh_interval = 5000)");
        String expectedSetSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.refresh_interval\":\"5000\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";
        JSONAssert.assertEquals(expectedSetSettings, getIndexSettings("test"), false);

        execute("ALTER TABLE test RESET (refresh_interval)");
        String expectedResetSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.refresh_interval\":\"1000\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";
        JSONAssert.assertEquals(expectedResetSettings, getIndexSettings("test"), false);
    }



    @Test
    public void testCreateTableAlreadyExistsException() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The table 'test' already exists.");
        execute("create table test (col1 integer primary key, col2 string)");
    }

    @Test
    public void testCreateTableWithReplicasAndShards() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)" +
                "clustered by (col1) into 10 shards with (number_of_replicas=2)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        String expectedMapping = "{\"default\":{" +
                "\"dynamic\":\"true\"," +
                "\"_meta\":{" +
                "\"primary_keys\":[\"col1\"]," +
                "\"routing\":\"col1\"}," +
                "\"_all\":{\"enabled\":false}," +
                "\"properties\":{" +
                "\"col1\":{\"type\":\"integer\",\"doc_values\":true}," +
                "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true}" +
                "}}}";

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"2\"," +
                "\"index.number_of_shards\":\"10\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);
    }

    @Test
    public void testCreateTableWithStrictColumnPolicy() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)" +
                "with (column_policy='strict')");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        String expectedMapping = "{\"default\":{" +
                "\"dynamic\":\"strict\"," +
                "\"_meta\":{" +
                "\"primary_keys\":[\"col1\"]}," +
                "\"_all\":{\"enabled\":false}," +
                "\"properties\":{" +
                "\"col1\":{\"type\":\"integer\",\"doc_values\":true}," +
                "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true}" +
                "}}}";

        String expectedSettings = "{\"test\":{" +
                "\"settings\":{" +
                "\"index.number_of_replicas\":\"1\"," +
                "\"index.number_of_shards\":\"5\"," +
                "\"index.version.created\":\"1030499\"" +
                "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);
    }

    @Test
    public void testCreateTableWithInlineDefaultIndex() throws Exception {
        execute("create table quotes (quote string index using plain) with (number_of_replicas = 0)");
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        refresh();

        // matching does not work on plain indexes
        execute("select quote from quotes where match(quote, 'time')");
        assertEquals(0, response.rowCount());

        // filtering on the actual value does work
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);
    }

    @Test
    public void testCreateTableWithInlineIndex() throws Exception {
        execute("create table quotes (quote string index using fulltext) with (number_of_replicas = 0)");
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        refresh();

        execute("select quote from quotes where match(quote, 'time')");
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);

        // filtering on the actual value does not work anymore because its now indexed using the
        // standard analyzer
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testCreateTableWithIndexOff() throws Exception {
        execute("create table quotes (id int, quote string index off) with (number_of_replicas = 0)");
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes (id, quote) values (?, ?)", new Object[]{1, quote});
        refresh();

        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(0, response.rowCount());

        execute("select quote from quotes where id = 1");
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);
    }

    @Test
    public void testCreateTableWithIndex() throws Exception {
        execute("create table quotes (quote string, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='english')) with (number_of_replicas = 0)");
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
                .actionGet().isExists());

        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        refresh();

        execute("select quote from quotes where match(quote_fulltext, 'time')");
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);

        // filtering on the actual value does still work
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testCreateTableWithCompositeIndex() throws Exception {
        execute("create table novels (title string, description string, " +
                "index title_desc_fulltext using fulltext(title, description) " +
                "with(analyzer='english')) with (number_of_replicas = 0)");
        ensureGreen();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("novels"))
                .actionGet().isExists());

        String title = "So Long, and Thanks for All the Fish";
        String description = "Many were increasingly of the opinion that they'd all made a big " +
                "mistake in coming down from the trees in the first place. And some said that " +
                "even the trees had been a bad move, and that no one should ever have left " +
                "the oceans.";
        execute("insert into novels (title, description) values(?, ?)",
                new Object[]{title, description});
        refresh();

        // match token existing at field `title`
        execute("select title, description from novels where match(title_desc_fulltext, 'fish')");
        assertEquals(1L, response.rowCount());
        assertEquals(title, response.rows()[0][0]);
        assertEquals(description, response.rows()[0][1]);

        // match token existing at field `description`
        execute("select title, description from novels where match(title_desc_fulltext, 'oceans')");
        assertEquals(1L, response.rowCount());
        assertEquals(title, response.rows()[0][0]);
        assertEquals(description, response.rows()[0][1]);

        // filtering on the actual values does still work
        execute("select title from novels where title = ?", new Object[]{title});
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testAlterTable() throws Exception {
        execute("create table test (col1 int) with (number_of_replicas='0-all')");
        ensureGreen();

        execute("select number_of_replicas from information_schema.tables where table_name = 'test'");
        assertEquals("0-all", response.rows()[0][0]);

        execute("alter table test set (number_of_replicas=0)");
        execute("select number_of_replicas from information_schema.tables where table_name = 'test'");
        assertEquals("0", response.rows()[0][0]);
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        execute("create table t (id int primary key) with (number_of_replicas=0)");
        execute("alter table t add column name string");

        execute("select data_type from information_schema.columns where " +
                "table_name = 't' and column_name = 'name'");
        assertThat((String) response.rows()[0][0], is("string"));

        execute("alter table t add column o object as (age int)");
        execute("select data_type from information_schema.columns where " +
                "table_name = 't' and column_name = 'o'");
        assertThat((String) response.rows()[0][0], is("object"));
    }



    @Test
    public void testAlterTableAddColumnAsPrimaryKey() throws Exception {
        execute("create table t (id int primary key) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureGreen();
        execute("alter table t add column name string primary key");
        execute("select constraint_name from information_schema.table_constraints " +
                "where table_name = 't' and schema_name = 'doc' and constraint_type = 'PRIMARY_KEY'");

        assertThat(response.rowCount(), is(1L));
        assertThat((String[]) response.rows()[0][0], equalTo(new String[]{"name", "id"}));
    }

    @Test
    public void testAlterTableAddObjectColumnToExistingObject() throws Exception {
        execute("create table t (o object as (x string)) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureGreen();
        execute("alter table t add o['y'] int");
        try {
            execute("alter table t add o object as (z string)");
            assertTrue(false);
        } catch (SQLActionException e) {
            // column o exists already
        }
        execute("select * from information_schema.columns where " +
                "table_name = 't' and schema_name='doc'" +
                "order by column_name asc");
        assertThat(response.rowCount(), is(3L));

        List<String> fqColumnNames = new ArrayList<>();
        for (Object[] row : response.rows()) {
            fqColumnNames.add((String) row[2]);
        }
        assertThat(fqColumnNames, Matchers.contains("o", "o['x']", "o['y']"));
    }

    @Test
    public void testDropTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();

        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());

        execute("drop table test");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        assertFalse(client().admin().indices().exists(new IndicesExistsRequest("test"))
                .actionGet().isExists());
    }

    @Test
    public void testDropUnknownTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table 'test' unknown");

        execute("drop table test");
    }

    @Test
    public void testCreateAlterAndDropBlobTable() throws Exception {
        execute("create blob table screenshots with (number_of_replicas=0)");
        execute("alter blob table screenshots set (number_of_replicas=1)");
        execute("select number_of_replicas from information_schema.tables " +
                "where schema_name = 'blob' and table_name = 'screenshots'");
        assertEquals("1", response.rows()[0][0]);
        execute("drop blob table screenshots");
    }

}
