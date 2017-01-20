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

import com.google.common.collect.ImmutableMap;
import io.crate.Version;
import io.crate.action.sql.SQLActionException;
import io.crate.metadata.PartitionName;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(randomDynamicTemplates = false)
@UseJdbc
public class DDLIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testCreateTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) " +
                "clustered into 5 shards with (number_of_replicas = 1)");
        ensureYellow();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
            .actionGet().isExists());

        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"true\",\"_meta\":{\"primary_keys\":[\"col1\"]}," +
                                 "\"_all\":{\"enabled\":false}," +
                                 "\"dynamic_templates\":[{\"strings\":{\"mapping\":{\"index\":\"not_analyzed\",\"store\":false,\"type\":\"string\",\"doc_values\":true},\"match_mapping_type\":\"string\"}}]," +
                                 "\"properties\":{" +
                                 "\"col1\":{\"type\":\"integer\"},\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\"}}}}";

        String expectedSettings = "{\"test\":{" +
                                  "\"settings\":{" +
                                  "\"index.number_of_replicas\":\"1\"," +
                                  "\"index.number_of_shards\":\"5\"," +
                                  "\"index.version.created\":\"" + Version.CURRENT.esVersion.id + "\"" +
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
        execute("create table test (id int primary key, content string) " +
                "clustered into 5 shards " +
                "with (refresh_interval=0, number_of_replicas = 0)");
        ensureYellow();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
            .actionGet().isExists());

        String expectedSettings = "{\"test\":{" +
                                  "\"settings\":{" +
                                  "\"index.number_of_replicas\":\"0\"," +
                                  "\"index.number_of_shards\":\"5\"," +
                                  "\"index.refresh_interval\":\"0ms\"," +
                                  "\"index.version.created\":\"" + Version.CURRENT.esVersion.id + "\"" +
                                  "}}}";
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);

        execute("ALTER TABLE test SET (refresh_interval = 5000)");
        String expectedSetSettings = "{\"test\":{" +
                                     "\"settings\":{" +
                                     "\"index.number_of_replicas\":\"0\"," +
                                     "\"index.number_of_shards\":\"5\"," +
                                     "\"index.refresh_interval\":\"5000ms\"," +
                                     "\"index.version.created\":\"" + Version.CURRENT.esVersion.id + "\"" +
                                     "}}}";
        JSONAssert.assertEquals(expectedSetSettings, getIndexSettings("test"), false);

        execute("ALTER TABLE test RESET (refresh_interval)");
        String expectedResetSettings = "{\"test\":{" +
                                       "\"settings\":{" +
                                       "\"index.number_of_replicas\":\"0\"," +
                                       "\"index.number_of_shards\":\"5\"," +
                                       "\"index.refresh_interval\":\"1000ms\"," +
                                       "\"index.version.created\":\"" + Version.CURRENT.esVersion.id + "\"" +
                                       "}}}";
        JSONAssert.assertEquals(expectedResetSettings, getIndexSettings("test"), false);
    }


    @Test
    public void testCreateTableAlreadyExistsException() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("The table 'doc.test' already exists.");
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
                                 "\"col1\":{\"type\":\"integer\"}," +
                                 "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\"}" +
                                 "}}}";

        String expectedSettings = "{\"test\":{" +
                                  "\"settings\":{" +
                                  "\"index.number_of_replicas\":\"2\"," +
                                  "\"index.number_of_shards\":\"10\"," +
                                  "\"index.version.created\":\"" + Version.CURRENT.esVersion.id + "\"" +
                                  "}}}";

        JSONAssert.assertEquals(expectedMapping, getIndexMapping("test"), JSONCompareMode.LENIENT);
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), JSONCompareMode.LENIENT);
    }

    @Test
    public void testCreateTableWithStrictColumnPolicy() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) " +
                "clustered into 5 shards " +
                "with (column_policy='strict', number_of_replicas = 0)");
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
            .actionGet().isExists());

        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{\"primary_keys\":[\"col1\"]},\"_all\":{\"enabled\":false}," +
                                 "\"dynamic_templates\":[{\"strings\":{\"mapping\":{\"index\":\"not_analyzed\",\"store\":false,\"type\":\"string\",\"doc_values\":true},\"match_mapping_type\":\"string\"}}]," +
                                 "\"properties\":{" +
                                 "\"col1\":{\"type\":\"integer\"}," +
                                 "\"col2\":{\"type\":\"string\",\"index\":\"not_analyzed\"}}}}";

        String expectedSettings = "{\"test\":{" +
                                  "\"settings\":{" +
                                  "\"index.number_of_replicas\":\"0\"," +
                                  "\"index.number_of_shards\":\"5\"," +
                                  "\"index.version.created\":\"" + Version.CURRENT.esVersion.id + "\"" +
                                  "}}}";


        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);
    }

    @Test
    public void testCreateGeoShapeExplicitIndex() throws Exception {
        execute("create table test (col1 geo_shape INDEX using QUADTREE with (precision='1m', distance_error_pct='0.25'))");
        ensureYellow();
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"true\"," +
                                 "\"_all\":{\"enabled\":false}," +
                                 "\"dynamic_templates\":[{\"strings\":{\"mapping\":{\"index\":\"not_analyzed\",\"store\":false,\"type\":\"string\",\"doc_values\":true},\"match_mapping_type\":\"string\"}}]," +
                                 "\"properties\":{" +
                                 "\"col1\":{\"type\":\"geo_shape\",\"tree\":\"quadtree\",\"precision\":\"1.0m\",\"distance_error_pct\":0.25}}}}";
        assertEquals(expectedMapping, getIndexMapping("test"));
    }

    @Test
    public void testCreateGeoShape() throws Exception {
        execute("create table test (col1 geo_shape)");
        ensureYellow();
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"true\"," +
                                 "\"_all\":{\"enabled\":false}," +
                                 "\"dynamic_templates\":[{\"strings\":{\"mapping\":{\"index\":\"not_analyzed\",\"store\":false,\"type\":\"string\",\"doc_values\":true},\"match_mapping_type\":\"string\"}}]," +
                                 "\"properties\":{\"col1\":{\"type\":\"geo_shape\"}}}}";
        assertEquals(expectedMapping, getIndexMapping("test"));

    }

    @Test
    public void testGeoShapeInvalidPrecision() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Value '10%' of setting precision is not a valid distance uni");
        execute("create table test (col1 geo_shape INDEX using QUADTREE with (precision='10%'))");
    }

    @Test
    public void testGeoShapeInvalidDistance() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Value 'true' of setting distance_error_pct is not a float value");
        execute("create table test (col1 geo_shape INDEX using QUADTREE with (distance_error_pct=true))");
    }

    @Test
    public void testUnknownGeoShapeSetting() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Setting \"does_not_exist\" ist not supported on geo_shape index");
        execute("create table test (col1 geo_shape INDEX using QUADTREE with (does_not_exist=false))");
    }

    @Test
    public void testCreateTableWithInlineDefaultIndex() throws Exception {
        execute("create table quotes (quote string index using plain) with (number_of_replicas = 0)");
        ensureYellow();
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
        ensureYellow();
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
        ensureYellow();
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
        ensureYellow();
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
        ensureYellow();
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
        ensureYellow();

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
        ensureYellow();
        execute("alter table t add column name string primary key");
        execute("select constraint_name from information_schema.table_constraints " +
                "where table_name = 't' and table_schema = 'doc' and constraint_type = 'PRIMARY_KEY'");

        assertThat(response.rowCount(), is(1L));
        assertThat((Object[]) response.rows()[0][0], arrayContaining(new Object[]{"name", "id"}));
    }

    @Test
    public void testAlterTableWithRecordsAddColumnAsPrimaryKey() throws Exception {
        execute("create table t (id int primary key) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (id) values(1)");
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot add a primary key column to a table that isn't empty");
        execute("alter table t add column name string primary key");
    }

    @Test
    public void testAlterTableWithoutRecordsAddGeneratedColumn() throws Exception {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("alter table t add column id_generated as (id + 1)");
        execute("insert into t (id) values(1)");
        refresh();
        execute("select id, id_generated from t");
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Long) response.rows()[0][1], is(2L));
    }

    @Test
    public void testAlterTableWithRecordsAddGeneratedColumn() throws Exception {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (id) values(1)");
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot add a generated column to a table that isn't empty");
        execute("alter table t add column id_generated as (id + 1)");
    }

    @Test
    public void testAlterTableAddObjectColumnToExistingObject() {
        execute("create table t (o object as (x string)) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("alter table t add o['y'] int");
        try {
            execute("alter table t add o object as (z string)");
            fail("did not fail for existing column o");
        } catch (SQLActionException e) {
            // column o exists already
            assertThat(e.getMessage(), containsString("The table doc.t already has a column named o"));
        }
        execute("select * from information_schema.columns where " +
                "table_name = 't' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response.rowCount(), is(3L));

        List<String> fqColumnNames = new ArrayList<>();
        for (Object[] row : response.rows()) {
            fqColumnNames.add((String) row[0]);
        }
        assertThat(fqColumnNames, Matchers.contains("o", "o['x']", "o['y']"));
    }

    @Test
    public void testAlterTableAddObjectColumnToExistingObjectNested() throws Exception {
        execute("CREATE TABLE my_table (" +
                "  name string, " +
                "  age integer," +
                "  book object as (isbn string)" +
                ")");
        ensureYellow();
        execute("alter table my_table add column book['author'] object as (\"authorId\" integer)");
        waitNoPendingTasksOnAll();
        execute("select column_name from information_schema.columns where " +
                "table_name = 'my_table' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response.rowCount(), is(6L));
        assertThat(TestingHelpers.getColumn(response.rows(), 0),
            is(Matchers.<Object>arrayContaining("age", "book", "book['author']", "book['author']['authorId']", "book['isbn']", "name")));
        execute("alter table my_table add column book['author']['authorName'] string");
        waitNoPendingTasksOnAll();
        execute("select column_name from information_schema.columns where " +
                "table_name = 'my_table' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response.rowCount(), is(7L));
        assertThat(TestingHelpers.getColumn(response.rows(), 0),
            is(Matchers.<Object>arrayContaining("age", "book", "book['author']", "book['author']['authorId']", "book['author']['authorName']", "book['isbn']", "name")));

    }

    @Test
    public void testAlterTableAddNestedObjectWithArrayToExistingObject() throws Exception {
        execute("CREATE TABLE my_table (col1 object)");
        ensureYellow();
        execute("ALTER TABLE my_table ADD COLUMN col1['col2'] object as (col3 array(string))");
        waitNoPendingTasksOnAll();
        execute("SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_name = 'my_table' AND table_schema = 'doc' " +
                "ORDER BY column_name asc");
        assertThat(TestingHelpers.getColumn(response.rows(), 0),
            is(Matchers.<Object>arrayContaining("col1", "col1['col2']", "col1['col2']['col3']")));
        assertThat(TestingHelpers.getColumn(response.rows(), 1),
            is(Matchers.<Object>arrayContaining("object", "object", "string_array")));

        execute("DROP TABLE my_table");
        ensureYellow();

        execute("CREATE TABLE my_table (col1 object as (col2 object))");
        ensureYellow();
        execute("ALTER TABLE my_table ADD COLUMN col1['col2']['col3'] object as (col4 array(long))");
        waitNoPendingTasksOnAll();
        execute("SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_name = 'my_table' AND table_schema = 'doc' " +
                "ORDER BY column_name asc");
        assertThat(TestingHelpers.getColumn(response.rows(), 0),
            is(Matchers.<Object>arrayContaining("col1", "col1['col2']", "col1['col2']['col3']", "col1['col2']['col3']['col4']")));
        assertThat(TestingHelpers.getColumn(response.rows(), 1),
            is(Matchers.<Object>arrayContaining("object", "object", "object", "long_array")));
    }

    @Test
    public void testAlterTableAddObjectToObjectArray() throws Exception {
        execute("CREATE TABLE t (" +
                "   attributes ARRAY(" +
                "       OBJECT (STRICT) as (" +
                "           name STRING" +
                "       )" +
                "   )" +
                ")");
        ensureYellow();
        execute("ALTER TABLE t ADD column attributes['is_nice'] BOOLEAN");
        execute("INSERT INTO t (attributes) values ([{name='Trillian', is_nice=True}])");
        refresh();
        execute("select attributes from t");
        assertThat(((Object[])response.rows()[0][0])[0], is(ImmutableMap.of("name", "Trillian", "is_nice", true)));
    }

    @Test
    @UseJdbc(0) // drop table has no rowcount
    public void testDropTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();

        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
            .actionGet().isExists());

        execute("drop table test");
        assertThat(response.rowCount(), is(1L));
        assertFalse(client().admin().indices().exists(new IndicesExistsRequest("test"))
            .actionGet().isExists());
    }

    @Test
    public void testDropTableIfExistsRaceCondition() throws Exception {
        execute("create table test (name string)");
        execute("drop table test");
        // could fail if the meta data update triggered by the previous drop table wasn't fully propagated in the cluster
        execute("drop table if exists test");
    }

    @Test
    public void testDropUnknownTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table 'doc.test' unknown");

        execute("drop table test");
    }

    @Test
    @UseJdbc(0) // drop table has no rowcount
    public void testDropTableIfExists() {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureYellow();

        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("test"))
            .actionGet().isExists());
        execute("drop table if exists test");
        assertThat(response.rowCount(), is(1L));
        assertFalse(client().admin().indices().exists(new IndicesExistsRequest("test"))
            .actionGet().isExists());
    }

    @Test
    public void testDropIfExistsUnknownTable() throws Exception {
        execute("drop table if exists nonexistent");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testCreateAlterAndDropBlobTable() throws Exception {
        execute("create blob table screenshots with (number_of_replicas=0)");
        execute("alter blob table screenshots set (number_of_replicas=1)");
        execute("select number_of_replicas from information_schema.tables " +
                "where table_schema = 'blob' and table_name = 'screenshots'");
        assertEquals("1", response.rows()[0][0]);
        execute("drop blob table screenshots");
    }

    @Test
    @UseJdbc(0) // drop table has no rowcount
    public void testDropIfExistsBlobTable() throws Exception {
        execute("create blob table screenshots with (number_of_replicas=0)");
        execute("drop blob table if exists screenshots");
        assertEquals(response.rowCount(), 1);
    }

    @Test
    public void testDropBlobTableIfExistsUnknownTable() throws Exception {
        execute("drop blob table if exists nonexistent");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testAlterShardsOfPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='0-all')");
        ensureYellow();

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );
        execute("alter table quotes set (number_of_shards=5)");

        String templateName = PartitionName.templateName(null, "quotes");
        GetIndexTemplatesResponse templatesResponse =
            client().admin().indices().prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(5));

        execute("insert into quotes (id, quote, date) values (?, ?, ?)",
            new Object[]{3, "Time is a illusion. Lunchtime doubles so", 1495961200000L}
        );
        String partition = new PartitionName("quotes", Arrays.asList(new BytesRef("1495961200000"))).asIndexName();
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(partition).execute().get();
        assertThat(settingsResponse.getSetting(partition, IndexMetaData.SETTING_NUMBER_OF_SHARDS), is("5"));
    }


    @Test
    public void testCreateTableWithCustomSchema() throws Exception {
        execute("create table a.t (name string) with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into a.t (name) values ('Ford')");
        assertThat(response.rowCount(), is(1L));
        refresh();

        execute("select name from a.t");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Ford"));

        execute("select table_schema from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("a"));
    }

    @Test
    public void testCreateTableWithIllegalCustomSchemaCheckedByES() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("table name \"AAA.t\" is invalid.");
        execute("create table \"AAA\".t (name string) with (number_of_replicas=0)");
    }

    @Test
    @UseJdbc(0) // drop table has no rowcount
    public void testDropTableWithCustomSchema() throws Exception {
        execute("create table a.t (name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("drop table a.t");
        assertThat(response.rowCount(), is(1L));

        execute("select table_schema from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testCreateTableWithGeneratedColumn() throws Exception {
        execute("create table test (ts timestamp, day as date_trunc('day', ts)) with (number_of_replicas=0)");
        ensureYellow();
        String expectedMapping = "{\"default\":" +
                                 "{\"dynamic\":\"true\"," +
                                 "\"_meta\":{\"generated_columns\":{\"day\":\"date_trunc('day', ts)\"}}," +
                                 "\"_all\":{\"enabled\":false}," +
                                 "\"dynamic_templates\":[{\"strings\":{\"mapping\":{\"index\":\"not_analyzed\",\"store\":false,\"type\":\"string\",\"doc_values\":true},\"match_mapping_type\":\"string\"}}]," +
                                 "\"properties\":{\"day\":{\"type\":\"date\",\"format\":\"epoch_millis||strict_date_optional_time\"},\"ts\":{\"type\":\"date\",\"format\":\"epoch_millis||strict_date_optional_time\"}}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
    }
}
