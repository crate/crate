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

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SQLActionException;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseRandomizedSchema;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static io.crate.Constants.DEFAULT_MAPPING_TYPE;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 2)
public class PartitionedTableIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);
    private String copyFilePath = Paths.get(getClass().getResource("/essetup/data/copy").toURI()).toUri().toString();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public PartitionedTableIntegrationTest() throws URISyntaxException {
    }

    @After
    public void resetSettings() {
        execute("RESET GLOBAL stats.enabled");
    }

    @Test
    public void testCopyFromIntoPartitionedTableWithPARTITIONKeyword() {
        execute("create table quotes (" +
                "   id integer primary key," +
                "   date timestamp with time zone primary key," +
                "   quote string index using fulltext) " +
                "partitioned by (date) with (number_of_replicas=0)");
        ensureYellow();
        execute("copy quotes partition (date=1400507539938) from ?", new Object[]{
            copyFilePath + "test_copy_from.json"});
        assertEquals(3L, response.rowCount());
        refresh();
        execute("select id, date, quote from quotes order by id asc");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0][0], is(1));
        assertThat(response.rows()[0][1], is(1400507539938L));
        assertThat(response.rows()[0][2], is("Don't pa\u00f1ic."));

        execute("select count(*) from information_schema.table_partitions where table_name = 'quotes'");
        assertThat((Long) response.rows()[0][0], is(1L));

        execute("copy quotes partition (date=1800507539938) from ?", new Object[]{
            copyFilePath + "test_copy_from.json"});
        refresh();

        execute("select partition_ident from information_schema.table_partitions " +
                "where table_name = 'quotes' " +
                "order by partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows()[0][0], is("04732d1g60qj0dpl6csjicpo"));
        assertThat(response.rows()[1][0], is("04732e1g60qj0dpl6csjicpo"));
    }


    /**
     * Test requires patch in ES 2.1 (https://github.com/crate/elasticsearch/commit/66564f88d21ad3d3be908dbe50974c448f7929d7)
     * or ES 2.x (https://github.com/elastic/elasticsearch/pull/16767).
     * Otherwise the rowCount returned from the copy from statement is ambiguous.
     */
    @Test
    public void testCopyFromIntoPartitionedTable() throws Exception {
        execute("create table quotes (" +
                "  id integer primary key, " +
                "  quote string index using fulltext" +
                ") partitioned by (id)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertEquals(3L, response.rowCount());
        refresh();
        ensureYellow();

        for (String id : ImmutableList.of("1", "2", "3")) {
            String partitionName = new PartitionName(
                new RelationName(sqlExecutor.getCurrentSchema(), "quotes"),
                ImmutableList.of(id)).asIndexName();
            assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName));
            assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).getAliases().get(getFqn("quotes")));
        }

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));
    }

    @Test
    public void testCopyFromIntoPartitionedTableWithGeneratedColumnPK() throws Exception {
        execute("create table quotes (" +
                "  id integer, " +
                "  quote string index using fulltext, " +
                "  gen_lower_quote string generated always as lower(quote), " +
                "  PRIMARY KEY(id, gen_lower_quote) " +
                ") partitioned by (gen_lower_quote)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertEquals(3L, response.rowCount());
        refresh();
        ensureYellow();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(3));
    }

    @Test
    public void testInsertIntoClosedPartition() throws Exception {
        execute("create table t (n integer) partitioned by (n)");
        execute("insert into t (n) values (1)");
        refresh();
        ensureYellow();

        execute("alter table t partition (n = 1) close");
        execute("insert into t (n) values (1)");
        assertThat(response.rowCount(), is(0L));
        refresh();

        execute("select count(*) from t");
        assertEquals(0L, response.rows()[0][0]);
    }

    @Test
    public void testSelectFromClosedPartition() throws Exception {
        execute("create table t (n integer) partitioned by (n)");
        ensureYellow();
        execute("insert into t (n) values (1)");
        ensureGreen();

        execute("alter table t partition (n = 1) close");
        execute("select count(*) from t");
        assertEquals(0L, response.rows()[0][0]);
    }

    @Test
    public void testCopyFromPartitionedTableCustomSchema() throws Exception {
        execute("create table my_schema.parted (" +
                "  id long, " +
                "  month timestamp with time zone, " +
                "  created timestamp with time zone) " +
                "partitioned by (month) with (number_of_replicas=0)");
        ensureGreen();
        File copyFromFile = folder.newFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(
            new FileOutputStream(copyFromFile),
            StandardCharsets.UTF_8)) {
            writer.write(
                "{\"id\":1, \"month\":1425168000000, \"created\":1425901500000}\n" +
                "{\"id\":2, \"month\":1420070400000,\"created\":1425901460000}");
        }
        String uriPath = Paths.get(copyFromFile.toURI()).toUri().toString();

        execute("copy my_schema.parted from ? with (shared=true)", new Object[]{uriPath});
        assertEquals(2L, response.rowCount());
        refresh();

        ensureGreen();
        waitNoPendingTasksOnAll();

        execute("select table_schema, table_name, number_of_shards, number_of_replicas, clustered_by, partitioned_by " +
                "from information_schema.tables where table_schema='my_schema' and table_name='parted'");
        assertThat(printedTable(response.rows()), is("my_schema| parted| 4| 0| _id| [month]\n"));

        // no other tables with that name, e.g. partitions considered as tables or such
        execute("select table_schema, table_name from information_schema.tables where table_name like '%parted%'");
        assertThat(printedTable(response.rows()), is(
            "my_schema| parted\n"));

        execute("select count(*) from my_schema.parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testCreatePartitionedTableAndQueryMeta() {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   timestamp timestamp with time zone" +
            ") partitioned by(timestamp) with (number_of_replicas=0)");

        execute("select * from information_schema.tables where table_schema = ? order by table_name", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][12], is("quotes"));
        assertThat(response.rows()[0][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        assertThat(response.rows()[0][1], is(false));
        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        execute("select * from information_schema.columns where table_name='quotes' order by ordinal_position");
        assertThat(response.rowCount(), is(3L));
        assertThat(response.rows()[0][11], is("id"));
        assertThat(response.rows()[1][11], is("quote"));
        assertThat(response.rows()[2][11], is("timestamp"));
    }

    @Test
    public void testInsertPartitionedTable() {
        execute(
            "create table parted (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone" +
            ") partitioned by (date)");
        ensureYellow();
        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "parted");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        assertThat(templatesResponse.getIndexTemplates().get(0).patterns(),
            contains(is(templateName + "*")));
        assertThat(templatesResponse.getIndexTemplates().get(0).name(),
            is(templateName));
        assertTrue(templatesResponse.getIndexTemplates().get(0).getAliases().get(getFqn("parted")) != null);

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{1, "Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
        refresh();

        assertTrue(clusterService().state().metaData().hasAlias(getFqn("parted")));

        String partitionName = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "parted"),
            Collections.singletonList(String.valueOf(13959981214861L))
        ).asIndexName();
        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet()
            .getState().metaData();
        assertNotNull(metaData.indices().get(partitionName).getAliases().get(getFqn("parted")));

        execute("select id, name, date from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((String) response.rows()[0][1], is("Ford"));
        assertThat((Long) response.rows()[0][2], is(13959981214861L));
    }

    private void validateInsertPartitionedTable() {
        execute("select table_name, partition_ident from information_schema.table_partitions order by 2 ");
        assertThat(
            printedTable(response.rows()),
            is("parted| 0400\n" +
               "parted| 04130\n" +
               "parted| 047j2cpp6ksjie1h68oj8e1m64\n"));
    }

    @Test
    public void testMultiValueInsertPartitionedTable() {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Ford", 13959981214861L,
                2, "Trillian", 0L,
                3, "Zaphod", null
            });
        assertThat(response.rowCount(), is(3L));
        ensureYellow();
        refresh();

        validateInsertPartitionedTable();
    }

    @Test
    public void testBulkInsertPartitionedTable() {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("insert into parted (id, name, date) values (?, ?, ?)", new Object[][]{
            new Object[]{1, "Ford", 13959981214861L},
            new Object[]{2, "Trillian", 0L},
            new Object[]{3, "Zaphod", null}
        });
        ensureYellow();
        refresh();

        validateInsertPartitionedTable();
    }

    @Test
    public void testInsertPartitionedTableOnlyPartitionedColumns() {
        execute("create table parted (name string, date timestamp with time zone)" +
                "partitioned by (name, date)");
        ensureYellow();

        execute("insert into parted (name, date) values (?, ?)",
            new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));

        execute("refresh table parted");

        PartitionName partitionName = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "parted"),
            Arrays.asList("Ford", String.valueOf(13959981214861L))
        );

        execute(
            "select count(*) from information_schema.table_partitions where partition_ident = ?",
            $(partitionName.ident()));
        assertThat(response.rows()[0][0], is(1L));

        execute("select date, name from parted");
        assertThat(
            printedTable(response.rows()),
            is("13959981214861| Ford\n"));
    }

    @Test
    public void testInsertPartitionedTableOnlyPartitionedColumnsAlreadyExists() {
        execute("create table parted (name string, date timestamp with time zone)" +
                "partitioned by (name, date)");
        ensureYellow();

        execute("insert into parted (name, date) values (?, ?)",
            new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
        refresh();
        execute("insert into parted (name, date) values (?, ?)",
            new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
        refresh();

        execute("select name, date from parted");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows()[0][0], is("Ford"));
        assertThat(response.rows()[1][0], is("Ford"));

        assertThat(response.rows()[0][1], is(13959981214861L));
        assertThat(response.rows()[1][1], is(13959981214861L));
    }

    @Test
    public void testInsertPartitionedTablePrimaryKeysDuplicate() {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp with time zone," +
                "  primary key (id, name)" +
                ") partitioned by (id, name)");
        ensureYellow();
        Long dateValue = System.currentTimeMillis();
        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{42, "Zaphod", dateValue});
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("A document with the same primary key exists already");

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{42, "Zaphod", 0L});
    }

    @Test
    public void testInsertPartitionedTableReversedPartitionedColumns() {
        execute(
            "create table parted (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone" +
            ") partitioned by (name, date)");
        ensureYellow();

        Long dateValue = System.currentTimeMillis();
        execute("insert into parted (id, date, name) values (?, ?, ?)",
            new Object[]{1, dateValue, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
        refresh();
        String partitionName = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "parted"),
            Arrays.asList("Trillian", dateValue.toString())).asIndexName();
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
            .getState().metaData().indices().get(partitionName).getAliases().get(getFqn("parted")));
    }

    @Test
    public void testInsertWithGeneratedColumnAsPartitionedColumn() {
        execute("create table parted_generated (" +
                " id integer," +
                " ts timestamp with time zone," +
                " day as date_trunc('day', ts)" +
                ") partitioned by (day)" +
                " with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into parted_generated (id, ts) values (?, ?)", new Object[]{
            1, "2015-11-23T14:43:00"
        });
        refresh();

        execute("select day from parted_generated");
        assertThat(response.rows()[0][0], is(1448236800000L));
    }

    @Test
    public void testSelectFromPartitionedTableWhereClause() {
        execute("create table quotes (id integer, quote string, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        refresh();

        execute("select id, quote from quotes where (timestamp = 1395961200000 or timestamp = 1395874800000) and id = 1");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testSelectCountFromPartitionedTableWhereClause() {
        execute("create table quotes (id integer, quote string, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        refresh();

        execute("select count(*) from quotes where (timestamp = 1395961200000 or timestamp = 1395874800000)");
        assertEquals(1L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);

        execute("select count(*) from quotes where timestamp = 1");
        assertEquals(0L, response.rows()[0][0]);
    }

    @Test
    public void testSelectFromPartitionedTable() {
        execute("create table quotes (id integer, quote string, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        refresh();
        execute("select id, quote, timestamp as ts, timestamp from quotes where timestamp > 1395874800000");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(2));
        assertThat(response.rows()[0][1], is("Time is an illusion. Lunchtime doubly so"));
        assertThat(response.rows()[0][2], is(1395961200000L));
    }

    @Test
    public void testSelectPrimaryKeyFromPartitionedTable() throws Exception {
        execute("create table stuff (" +
                "  id integer primary key, " +
                "  type byte primary key," +
                "  content string) " +
                "partitioned by(type) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
            new Object[]{1, 127, "Don't panic"});
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
            new Object[]{2, 126, "Time is an illusion. Lunchtime doubly so"});
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
            new Object[]{3, 126, "Now panic"});
        ensureYellow();

        execute("select id, type, content from stuff where id=2 and type=126");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(2));
        byte b = 126;
        assertThat((Byte) response.rows()[0][1], is(b));
        assertThat((String) response.rows()[0][2], is("Time is an illusion. Lunchtime doubly so"));

        // multiget
        execute("select id, type, content from stuff where id in (2, 3) and type=126 order by id");
        assertThat(response.rowCount(), is(2L));

        assertThat((Integer) response.rows()[0][0], is(2));
        assertThat((Byte) response.rows()[0][1], is(b));
        assertThat((String) response.rows()[0][2], is("Time is an illusion. Lunchtime doubly so"));

        assertThat((Integer) response.rows()[1][0], is(3));
        assertThat((Byte) response.rows()[1][1], is(b));
        assertThat((String) response.rows()[1][2], is("Now panic"));
    }

    @Test
    public void testUpdatePartitionedTable() {
        execute("create table quotes (id integer, quote string, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        refresh();

        execute("update quotes set quote = ? where timestamp = ?",
            new Object[]{"I'd far rather be happy than right any day.", 1395874800000L});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select id, quote from quotes where timestamp = 1395874800000");
        assertEquals(1L, response.rowCount());
        assertEquals(1, response.rows()[0][0]);
        assertEquals("I'd far rather be happy than right any day.", response.rows()[0][1]);

        execute("update quotes set quote = ?",
            new Object[]{"Don't panic"});
        assertEquals(2L, response.rowCount());
        refresh();

        execute("select id, quote from quotes where quote = ?",
            new Object[]{"Don't panic"});
        assertEquals(2L, response.rowCount());
    }

    @Test
    public void testUpdatePartitionedUnknownPartition() {
        execute(
            "create table quotes (" +
            "   id integer, " +
            "   quote string, " +
            "   timestamp timestamp with time zone, " +
            "   o object" +
            ") partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        refresh();

        execute("update quotes set quote='now panic' where timestamp = ?", new Object[]{1395874800123L});
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdatePartitionedUnknownColumn() {
        execute(
            "create table quotes (" +
            "   id integer, " +
            "   quote string, " +
            "   timestamp timestamp with time zone, " +
            "   o object(ignored)" +
            ") partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        refresh();

        execute("update quotes set quote='now panic' where o['timestamp'] = ?", new Object[]{1395874800123L});
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdatePartitionedUnknownColumnKnownValue() {
        execute("create table quotes (id integer, quote string, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        refresh();

        execute("update quotes set quote='now panic' where timestamp = ? and quote=?",
            new Object[]{1395874800123L, "Don't panic"});
        assertThat(response.rowCount(), is(0L));
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdateUnknownColumnKnownValueAndConjunction() {
        execute("create table quotes (id integer, quote string, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Don't panic", 1395961200000L});
        ensureYellow();
        refresh();

        execute("update quotes set quote='now panic' where not timestamp = ? and quote=?",
            new Object[]{1395874800000L, "Don't panic"});
        refresh();
        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testUpdateByQueryOnEmptyPartitionedTable() {
        execute("create table empty_parted(id integer, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();

        execute("update empty_parted set id = 10 where timestamp = 1396303200000");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testDeleteFromPartitionedTable() {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string, " +
            "   timestamp timestamp with time zone" +
            ") partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        ensureYellow();
        refresh();

        execute("delete from quotes where timestamp = 1395874800000 and id = 1");
        assertEquals(1, response.rowCount());
        refresh();

        execute("select id, quote from quotes where timestamp = 1395874800000");
        assertEquals(0L, response.rowCount());

        execute("select id, quote from quotes");
        assertEquals(2L, response.rowCount());

        execute("delete from quotes");
        assertThat(response.rowCount(), anyOf(is(0L), is(-1L)));
        refresh();

        execute("select id, quote from quotes");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testDeleteFromPartitionedTableUnknownPartition() throws Exception {
        this.setup.partitionTableSetup();
        String defaultSchema = sqlExecutor.getCurrentSchema();
        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                                       "where table_name='parted' and table_schema = ? " +
                                       "order by partition_ident", new Object[]{defaultSchema});
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is(new PartitionName(
            new RelationName(defaultSchema, "parted"), ImmutableList.of("1388534400000")).ident()));
        assertThat((String) response.rows()[1][0], is(new PartitionName(
            new RelationName(defaultSchema, "parted"), ImmutableList.of("1391212800000")).ident()));

        execute("delete from parted where date = '2014-03-01'");
        refresh();
        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                                          "where table_name='parted' and table_schema = ? " +
                                          "order by partition_ident", new Object[]{defaultSchema});
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableWrongPartitionedColumn() throws Exception {
        this.setup.partitionTableSetup();
        String defaultSchema = sqlExecutor.getCurrentSchema();
        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                                       "where table_name='parted' and table_schema = ? " +
                                       "order by partition_ident", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is(new PartitionName(
            new RelationName(defaultSchema, "parted"), ImmutableList.of("1388534400000")).ident()));
        assertThat((String) response.rows()[1][0], is(new PartitionName(
            new RelationName(defaultSchema, "parted"), ImmutableList.of("1391212800000")).ident()));

        execute("delete from parted where o['dat'] = '2014-03-01'");
        refresh();
        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                                          "where table_name='parted' and table_schema = ? " +
                                          "order by partition_ident", new Object[]{defaultSchema});
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableDeleteByQuery() {
        String defaultSchema = sqlExecutor.getCurrentSchema();
        execute("create table quotes (" +
                "   id integer, " +
                "   quote string, " +
                "   timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{4, "Now panic", 1395874800000L});
        ensureYellow();
        refresh();

        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                                       "where table_name='quotes' and table_schema = ? " +
                                       "order by partition_ident", new Object[]{defaultSchema});
        assertThat(response.rowCount(), is(3L));
        assertThat((String) response.rows()[0][0], is(new PartitionName(
            new RelationName(defaultSchema, "parted"), ImmutableList.of("1395874800000")).ident()));
        assertThat((String) response.rows()[1][0], is(new PartitionName(
            new RelationName(defaultSchema, "parted"), ImmutableList.of("1395961200000")).ident()));
        assertThat((String) response.rows()[2][0], is(new PartitionName(
            new RelationName(defaultSchema, "parted"), ImmutableList.of("1396303200000")).ident()));

        execute("delete from quotes where quote = 'Don''t panic'");
        refresh();

        execute("select * from quotes where quote = 'Don''t panic'");
        assertThat(this.response.rowCount(), is(0L));

        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                                          "where table_name='quotes' and table_schema = ? " +
                                          "order by partition_ident", new Object[]{defaultSchema});
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableDeleteByPartitionAndByQuery() {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   timestamp timestamp with time zone," +
            "   o object(ignored)" +
            ") partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{4, "Now panic", 1395874800000L});
        ensureYellow();
        refresh();

        // does not match
        execute("delete from quotes where quote = 'Don''t panic' and timestamp=?", new Object[]{1396303200000L});
        refresh();
        execute("select * from quotes where timestamp=?", new Object[]{1396303200000L});
        assertThat(response.rowCount(), is(1L));

        // matches
        execute("delete from quotes where quote = 'I''d far rather be happy than right any day' and timestamp=?", new Object[]{1396303200000L});
        refresh();
        execute("select * from quotes where timestamp=?", new Object[]{1396303200000L});
        assertThat(response.rowCount(), is(0L));

        execute("delete from quotes where timestamp=? and o['x']=5", new Object[]{1395874800000L});
        refresh();
        execute("select * from quotes where timestamp=?", new Object[]{1395874800000L});
        assertThat(response.rowCount(), is(2L));


    }

    @Test
    public void testDeleteFromPartitionedTableDeleteByPartitionAndQueryWithConjunction() {
        execute("create table quotes (id integer, quote string, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{2, "Don't panic", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
            new Object[]{3, "Don't panic", 1396303200000L});
        refresh();

        execute("delete from quotes where not timestamp=? and quote=?", new Object[]{1396303200000L, "Don't panic"});
        refresh();
        execute("select * from quotes");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testDeleteByQueryFromEmptyPartitionedTable() {
        execute("create table empty_parted (id integer, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();

        execute("delete from empty_parted where not timestamp = 1396303200000");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testGlobalAggregatePartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(0L));
        assertThat((Long) response.rows()[0][1], is(0L));
        assertNull(response.rows()[0][2]);
        assertNull(response.rows()[0][3]);
        assertNull(response.rows()[0][4]);
        assertNull(response.rows()[0][5]);

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{0, "Trillian", 100L});
        ensureYellow();
        refresh();

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(1L));
        assertThat((Long) response.rows()[0][1], is(1L));
        assertThat((Long) response.rows()[0][2], is(100L));
        assertThat((Long) response.rows()[0][3], is(100L));
        assertThat((Long) response.rows()[0][4], is(100L));
        assertThat((Double) response.rows()[0][5], is(100.0));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{1, "Ford", 1001L});
        ensureYellow();
        refresh();

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{2, "Arthur", 1001L});
        ensureYellow();
        refresh();

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));
        assertThat((Long) response.rows()[0][1], is(3L));
        assertThat((Long) response.rows()[0][2], is(100L));
        assertThat((Long) response.rows()[0][3], is(1001L));
        assertThat((Long) response.rows()[0][4], isOneOf(100L, 1001L));
        assertThat((Double) response.rows()[0][5], is(700.6666666666666));
    }

    @Test
    public void testGroupByPartitionedColumns() {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("select date, count(*) from parted group by date");
        assertThat(response.rowCount(), is(0L));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{0, "Trillian", 100L});
        ensureYellow();
        refresh();

        execute("select date, count(*) from parted group by date");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(100L));
        assertThat((Long) response.rows()[0][1], is(1L));

        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Arthur", null,
                2, "Ford", null
            });
        ensureYellow();
        refresh();

        execute("select date, count(*) from parted group by date order by count(*) desc");
        assertThat(response.rowCount(), is(2L));
        assertNull(response.rows()[0][0]);
        assertThat((Long) response.rows()[0][1], is(2L));
        assertThat((Long) response.rows()[1][0], is(100L));
        assertThat((Long) response.rows()[1][1], is(1L));
    }

    @Test
    public void testGroupByPartitionedColumnWhereClause() {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("select date, count(*) from parted where date > 0 group by date");
        assertThat(response.rowCount(), is(0L));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{0, "Trillian", 100L});
        ensureYellow();
        refresh();

        execute("select date, count(*) from parted where date > 0 group by date");
        assertThat(response.rowCount(), is(1L));

        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Arthur", 0L,
                2, "Ford", 2437646253L
            }
        );
        ensureYellow();
        refresh();

        execute("select date, count(*) from parted where date > 100 group by date");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(2437646253L));
        assertThat(response.rows()[0][1], is(1L));
    }

    @Test
    public void testGlobalAggregateWhereClause() {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted where date > 0");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(0L));
        assertThat(response.rows()[0][1], is(0L));
        assertNull(response.rows()[0][2]);
        assertNull(response.rows()[0][3]);
        assertNull(response.rows()[0][4]);
        assertNull(response.rows()[0][5]);

        execute("insert into parted (id, name, date) values " +
                "(?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Arthur", 0L,
                2, "Ford", 2437646253L,
                3, "Zaphod", 1L,
                4, "Trillian", 0L
            });
        assertThat(response.rowCount(), is(4L));
        ensureYellow();
        refresh();

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted where date > 0");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));
        assertThat((Long) response.rows()[0][1], is(2L));
        assertThat((Long) response.rows()[0][2], is(1L));
        assertThat((Long) response.rows()[0][3], is(2437646253L));
        assertThat((Long) response.rows()[0][4], isOneOf(1L, 2437646253L));
        assertThat((Double) response.rows()[0][5], is(1.218823127E9));
    }

    @Test
    public void testDropPartitionedTable() throws Exception {
        execute("create table quotes (" +
                "  id integer, " +
                "  quote string, " +
                "  date timestamp with time zone" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, date) values(?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L,
                2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        refresh();

        execute("drop table quotes");
        assertEquals(1L, response.rowCount());

        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices()
            .prepareGetTemplates(PartitionName.templateName(Schemas.DOC_SCHEMA_NAME, "quotes")).execute().get();
        assertThat(getIndexTemplatesResponse.getIndexTemplates().size(), is(0));

        ClusterState state = internalCluster().clusterService().state();
        assertThat(state.metaData().indices().size(), is(0));
        assertThat(state.metaData().hasAlias("quotes"), is(false));
    }

    @Test
    public void testPartitionedTableSelectById() throws Exception {
        execute("create table quotes (id integer, quote string, num double, primary key (id, num)) partitioned by (num)");
        ensureYellow();
        execute("insert into quotes (id, quote, num) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Don't panic", 4.0d,
                2, "Time is an illusion. Lunchtime doubly so", -4.0d
            });
        ensureYellow();
        refresh();
        execute("select * from quotes where id = 1 and num = 4");
        assertThat(response.rowCount(), is(1L));
        assertThat(String.join(", ", response.cols()), is("id, quote, num"));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((String) response.rows()[0][1], is("Don't panic"));
        assertThat((Double) response.rows()[0][2], is(4.0d));
    }

    @Test
    public void testInsertDynamicToPartitionedTable() throws Exception {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   date timestamp with time zone," +
            "   author object(dynamic) as (name string)" +
            ") partitioned by(date) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, date, author) values(?, ?, ?, ?), (?, ?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L,
                new HashMap<String, Object>() {{
                    put("name", "Douglas");
                }},
                2, "Time is an illusion. Lunchtime doubly so", 1395961200000L,
                new HashMap<String, Object>() {{
                    put("name", "Ford");
                }}});
        ensureYellow();
        refresh();

        execute("select * from information_schema.columns where table_name = 'quotes'");
        assertEquals(5L, response.rowCount());

        execute("insert into quotes (id, quote, date, author) values(?, ?, ?, ?)",
            new Object[]{3, "I'd far rather be happy than right any day", 1395874800000L,
                new HashMap<String, Object>() {{
                    put("name", "Douglas");
                    put("surname", "Adams");
                }}});
        ensureYellow();
        refresh();
        waitForMappingUpdateOnAll("quotes", "author.surname");

        execute("select * from information_schema.columns where table_name = 'quotes'");
        assertEquals(6L, response.rowCount());

        execute("select author['surname'] from quotes order by id");
        assertEquals(3L, response.rowCount());
        assertNull(response.rows()[0][0]);
        assertNull(response.rows()[1][0]);
        assertEquals("Adams", response.rows()[2][0]);
    }

    @Test
    public void testPartitionedTableAllConstraintsRoundTrip() {
        execute("create table quotes (id integer primary key, quote string, " +
                "date timestamp with time zone primary key, user_id string primary key) " +
                "partitioned by(date, user_id) clustered by (id) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L, "Arthur"});
        assertEquals(1L, response.rowCount());
        execute("insert into quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L, "Ford"});
        assertEquals(1L, response.rowCount());
        ensureYellow();
        refresh();

        execute("select id, quote from quotes where user_id = 'Arthur'");
        assertEquals(1L, response.rowCount());

        execute("update quotes set quote = ? where user_id = ?",
            new Object[]{"I'd far rather be happy than right any day", "Arthur"});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("delete from quotes where user_id = 'Arthur' and id = 1 and date = 1395874800000");
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(1L, response.rowCount());

        execute("delete from quotes"); // this will delete all partitions
        execute("delete from quotes"); // this should still work even though only the template exists

        execute("drop table quotes");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testPartitionedTableSchemaAllConstraintsRoundTrip() {
        execute("create table my_schema.quotes (id integer primary key, quote string, " +
                "date timestamp with time zone primary key, user_id string primary key) " +
                "partitioned by(date, user_id) clustered by (id) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into my_schema.quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L, "Arthur"});
        assertEquals(1L, response.rowCount());
        execute("insert into my_schema.quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L, "Ford"});
        assertEquals(1L, response.rowCount());
        ensureYellow();
        refresh();

        execute("select id, quote from my_schema.quotes where user_id = 'Arthur'");
        assertEquals(1L, response.rowCount());

        execute("update my_schema.quotes set quote = ? where user_id = ?",
            new Object[]{"I'd far rather be happy than right any day", "Arthur"});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("delete from my_schema.quotes where user_id = 'Arthur' and id = 1 and date = 1395874800000");
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select * from my_schema.quotes");
        assertEquals(1L, response.rowCount());

        execute("delete from my_schema.quotes"); // this will delete all partitions
        execute("delete from my_schema.quotes"); // this should still work even though only the template exists

        execute("drop table my_schema.quotes");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testPartitionedTableSchemaUpdateSameColumnNumber() throws Exception {
        execute("create table t1 (" +
                "   id int primary key," +
                "   date timestamp with time zone primary key" +
                ") partitioned by (date) with (number_of_replicas=0, column_policy = 'dynamic')");
        ensureYellow();
        execute("insert into t1 (id, date, dynamic_added_col1) values (1, '2014-01-01', 'foo')");
        execute("insert into t1 (id, date, dynamic_added_col2) values (2, '2014-02-01', 'bar')");
        refresh();
        ensureYellow();

        // schema updates are async and cannot reliably be forced
        int retry = 0;
        while (retry < 100) {
            execute("select * from t1");
            if (response.cols().length == 4) { // at some point both foo and bar columns must be present
                break;
            }
            Thread.sleep(100);
            retry++;
        }
        assertTrue(retry < 100);
    }

    @Test
    public void testPartitionedTableNestedAllConstraintsRoundTrip()  {
        execute("create table quotes (" +
                "id integer, " +
                "quote string, " +
                "created object as(" +
                "  date timestamp with time zone, " +
                "  user_id string)" +
                ") partitioned by(created['date']) clustered by (id) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
            new Object[]{1, "Don't panic", Map.of("date", 1395874800000L, "user_id", "Arthur")});
        assertEquals(1L, response.rowCount());
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", Map.of("date", 1395961200000L, "user_id", "Ford")});
        assertEquals(1L, response.rowCount());
        ensureYellow();
        refresh();

        execute("select id, quote, created['date'] from quotes where created['user_id'] = 'Arthur'");
        assertEquals(1L, response.rowCount());
        assertThat((Long) response.rows()[0][2], is(1395874800000L));

        execute("update quotes set quote = ? where created['date'] = ?",
            new Object[]{"I'd far rather be happy than right any day", 1395874800000L});
        assertEquals(1L, response.rowCount());

        execute("refresh table quotes");

        execute("select count(*) from quotes where quote=?", new Object[]{"I'd far rather be happy than right any day"});
        assertThat((Long) response.rows()[0][0], is(1L));

        execute("delete from quotes where created['user_id'] = 'Arthur' and id = 1 and created['date'] = 1395874800000");
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(1L, response.rowCount());

        execute("drop table quotes");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testAlterNumberOfReplicas() {
        String defaultSchema = sqlExecutor.getCurrentSchema();
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   date timestamp with time zone" +
            ") partitioned by(date) " +
            "clustered into 3 shards with (number_of_replicas='0-all')");
        ensureYellow();

        String templateName = PartitionName.templateName(defaultSchema, "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));

        execute("alter table quotes set (number_of_replicas=0)");
        ensureYellow();

        templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1), is(0));
        assertThat(templateSettings.getAsBoolean(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, true), is(false));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureYellow();
        refresh();

        assertTrue(clusterService().state().metaData().hasAlias(getFqn("quotes")));

        List<String> partitions = ImmutableList.of(
            new PartitionName(
                new RelationName(defaultSchema, "quotes"),
                Collections.singletonList("1395874800000")).asIndexName(),
            new PartitionName(
                new RelationName(defaultSchema, "quotes"),
                Collections.singletonList("1395961200000")).asIndexName()
        );

        execute("select number_of_replicas from information_schema.table_partitions");
        assertThat(
            printedTable(response.rows()),
            is("0\n" +
               "0\n"));

        execute("select number_of_replicas, number_of_shards from information_schema.tables where table_name = 'quotes'");
        assertEquals("0", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);

        execute("alter table quotes set (number_of_replicas='1-all')");
        ensureYellow();

        execute("select number_of_replicas from information_schema.tables where table_name = 'quotes'");
        assertEquals("1-all", response.rows()[0][0]);

        templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("1-all"));


        execute("select number_of_replicas from information_schema.table_partitions");
        assertThat(
            printedTable(response.rows()),
            is("1-all\n" +
               "1-all\n"));
    }

    @Test
    public void testAlterTableResetEmptyPartitionedTable() {
        execute("create table quotes (id integer, quote string, date timestamp with time zone) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='1')");
        ensureYellow();

        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));

        execute("alter table quotes reset (number_of_replicas)");
        ensureYellow();

        templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(0));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-1"));

    }

    @Test
    public void testAlterTableResetPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp with time zone) " +
                "partitioned by(date) clustered into 3 shards with( number_of_replicas = '1-all')");
        ensureYellow();

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureYellow();
        refresh();

        execute("alter table quotes reset (number_of_replicas)");
        ensureYellow();

        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(0));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-1"));
        assertBusy(() -> {
            execute("select number_of_replicas from information_schema.table_partitions");
            assertThat(
                printedTable(response.rows()),
                is("0-1\n" +
                   "0-1\n"));
        });
    }

    @Test
    public void testAlterPartitionedTablePartition() {
        execute("create table quotes (id integer, quote string, date timestamp with time zone) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureYellow();
        refresh();

        execute("alter table quotes partition (date=1395874800000) set (number_of_replicas=1)");
        ensureYellow();

        execute("select partition_ident, number_of_replicas from information_schema.table_partitions order by 1");
        assertThat(
            printedTable(response.rows()),
            is("04732cpp6ks3ed1o60o30c1g| 1\n" +
               "04732cpp6ksjcc9i60o30c1g| 0\n")
        );

        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(0));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));

    }

    @Test
    public void testAlterPartitionedTableSettings() throws Exception {
        execute("create table attrs (name string, attr string, value integer) " +
                "partitioned by (name) clustered into 1 shards with (number_of_replicas=0, \"routing.allocation.total_shards_per_node\"=5)");
        ensureYellow();
        execute("insert into attrs (name, attr, value) values (?, ?, ?), (?, ?, ?)",
            new Object[]{"foo", "shards", 1, "bar", "replicas", 2});
        refresh();
        execute("select settings['routing']['allocation'] from information_schema.table_partitions where table_name='attrs'");
        HashMap<String, Object> routingAllocation = new HashMap<String, Object>() {{
            put("enable", "all");
            put("total_shards_per_node", 5);
        }};
        assertEquals(routingAllocation, response.rows()[0][0]);
        assertEquals(routingAllocation, response.rows()[1][0]);

        execute("alter table attrs set (\"routing.allocation.total_shards_per_node\"=1)");
        execute("select settings['routing']['allocation'] from information_schema.table_partitions where table_name='attrs'");
        routingAllocation = new HashMap<String, Object>() {{
            put("enable", "all");
            put("total_shards_per_node", 1);
        }};
        assertEquals(routingAllocation, response.rows()[0][0]);
        assertEquals(routingAllocation, response.rows()[1][0]);
    }

    @Test
    public void testAlterPartitionedTableOnlySettings() throws Exception {
        execute("create table attrs (name string, attr string, value integer) " +
                "partitioned by (name) clustered into 1 shards with (number_of_replicas=0, \"routing.allocation.total_shards_per_node\"=5)");
        ensureYellow();
        execute("insert into attrs (name, attr, value) values (?, ?, ?), (?, ?, ?)",
            new Object[]{"foo", "shards", 1, "bar", "replicas", 2});
        refresh();

        execute("alter table ONLY attrs set (\"routing.allocation.total_shards_per_node\"=1)");

        // setting is not changed for existing partitions
        execute("select settings['routing']['allocation']['total_shards_per_node'] from information_schema.table_partitions where table_name='attrs'");
        assertThat((Integer) response.rows()[0][0], is(5));
        assertThat((Integer) response.rows()[1][0], is(5));

        // new partitions must use new settings
        execute("insert into attrs (name, attr, value) values (?, ?, ?), (?, ?, ?)",
            new Object[]{"Arthur", "shards", 1, "Ford", "replicas", 2});
        refresh();

        execute("select settings['routing']['allocation']['total_shards_per_node'] from information_schema.table_partitions where table_name='attrs' order by 1");
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer) response.rows()[1][0], is(1));
        assertThat((Integer) response.rows()[2][0], is(5));
        assertThat((Integer) response.rows()[3][0], is(5));
    }

    @Test
    public void testRefreshPartitionedTableAllPartitions() {
        execute("create table parted (id integer, name string, date timestamp with time zone) " +
            "partitioned by (date) with (refresh_interval=0)");

        execute("refresh table parted");
        assertThat(response.rowCount(), anyOf(is(0L), is(-1L)));

        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01'), " +
                "(2, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));
        ensureYellow();

        // cannot tell what rows are visible
        // could be none, could be all
        execute("select count(*) from parted");
        // cannot exactly tell which rows are visible
        assertThat((Long) response.rows()[0][0], lessThanOrEqualTo(2L));

        execute("refresh table parted");
        assertThat(response.rowCount(), is(2L));

        // assert that all is available after refresh
        execute("select count(*) from parted");
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testRefreshEmptyPartitionedTable() {
        execute(
            "create table parted (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone" +
            ") partitioned by (date) with (refresh_interval=0)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format("No partition for table '%s' with ident '04130' exists", getFqn("parted")));
        execute("refresh table parted partition(date=0)");
    }

    @Test
    public void testRefreshPartitionedTableSinglePartitions() {
        execute(
            "create table parted (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone" +
            ") partitioned by (date) " +
            "with (number_of_replicas=0, refresh_interval=-1)");
        ensureYellow();
        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01')," +
                "(2, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));

        ensureYellow();
        execute("refresh table parted");
        assertThat(response.rowCount(), is(2L));

        // assert that after refresh all columns are available
        execute("select * from parted");
        assertThat(response.rowCount(), is(2L));

        execute("insert into parted (id, name, date) values " +
                "(3, 'Zaphod', '1970-01-01')," +
                "(4, 'Marvin', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));

        // cannot exactly tell which rows are visible
        execute("select * from parted");
        // cannot exactly tell how much rows are visible at this point
        assertThat(response.rowCount(), lessThanOrEqualTo(4L));

        execute("refresh table parted PARTITION (date='1970-01-01')");
        assertThat(response.rowCount(), is(1L));

        // assert all partition rows are available after refresh
        execute("select * from parted where date='1970-01-01'");
        assertThat(response.rowCount(), is(2L));

        execute("refresh table parted PARTITION (date='1970-01-07')");
        assertThat(response.rowCount(), is(1L));

        // assert all partition rows are available after refresh
        execute("select * from parted where date='1970-01-07'");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testRefreshMultipleTablesWithPartition() {
        execute("create table t1 (" +
                "  id integer, " +
                "  name string, " +
                "  age integer, " +
                "  date timestamp with time zone)" +
                "  partitioned by (date, age)" +
                "  with (number_of_replicas=0, refresh_interval=-1)");
        ensureYellow();

        execute("insert into t1 (id, name, age, date) values " +
                "(1, 'Trillian', 90, '1970-01-01')," +
                "(2, 'Marvin', 50, '1970-01-07')," +
                "(3, 'Arthur', 50, '1970-01-07')," +
                "(4, 'Zaphod', 90, '1970-01-01')");
        assertThat(response.rowCount(), is(4L));

        execute("select * from t1 where age in (50, 90)");
        assertThat(response.rowCount(), lessThanOrEqualTo(2L));

        execute("refresh table t1 partition (age=50, date='1970-01-07'), " +
                "              t1 partition (age=90, date='1970-01-01')");
        assertThat(response.rowCount(), is(2L));

        execute("select * from t1 where age in (50, 90) and date in ('1970-01-07', '1970-01-01')");
        assertThat(response.rowCount(), lessThanOrEqualTo(4L));
    }

    @Test
    public void testAlterPartitionedTableKeepsMetadata() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer, " +
                "  score double" +
                ") partitioned by (score) with (number_of_replicas=0, column_policy='dynamic')");
        ensureGreen();
        execute("insert into dynamic_table (id, score) values (1, 10)");
        execute("refresh table dynamic_table");
        ensureGreen();
        MappingMetaData partitionMetaData = clusterService().state().metaData().indices()
            .get(new PartitionName(
                new RelationName(sqlExecutor.getCurrentSchema(), "dynamic_table"),
                Collections.singletonList("10.0")).asIndexName())
            .getMappings().get(DEFAULT_MAPPING_TYPE);
        Map<String, Object> metaMap = (Map) partitionMetaData.getSourceAsMap().get("_meta");
        assertThat(String.valueOf(metaMap.get("partitioned_by")), Matchers.is("[[score, double]]"));
        execute("alter table dynamic_table set (column_policy= 'dynamic')");
        waitNoPendingTasksOnAll();
        partitionMetaData = clusterService().state().metaData().indices()
            .get(new PartitionName(
                new RelationName(sqlExecutor.getCurrentSchema(), "dynamic_table"),
                Collections.singletonList("10.0")).asIndexName())
            .getMappings().get(DEFAULT_MAPPING_TYPE);
        metaMap = (Map) partitionMetaData.getSourceAsMap().get("_meta");
        assertThat(String.valueOf(metaMap.get("partitioned_by")), Matchers.is("[[score, double]]"));
    }

    @Test
    public void testCountPartitionedTable() {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp with time zone" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureYellow();

        execute("select count(*) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(0L));

        execute("insert into parted (id, name, date) values (1, 'Trillian', '1970-01-01'), (2, 'Ford', '2010-01-01')");
        ensureYellow();
        execute("refresh table parted");

        execute("select count(*) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(2L));
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testAlterTableAddColumnOnPartitionedTableWithoutPartitions() throws Exception {
        execute("create table t (id int primary key, date timestamp with time zone primary key) " +
                "partitioned by (date) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();

        execute("alter table t add column name string");
        execute("alter table t add column ft_name string index using fulltext");
        ensureYellow();

        execute("select * from t");
        assertThat(Arrays.asList(response.cols()), Matchers.containsInAnyOrder("date", "ft_name", "id", "name"));

        GetIndexTemplatesResponse templatesResponse = client().admin().indices().getTemplates(new GetIndexTemplatesRequest(".partitioned.t.")).actionGet();
        IndexTemplateMetaData metaData = templatesResponse.getIndexTemplates().get(0);
        String mappingSource = metaData.mappings().get(DEFAULT_MAPPING_TYPE).toString();
        Map mapping = (Map) XContentFactory.xContent(mappingSource)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingSource)
            .map()
            .get(DEFAULT_MAPPING_TYPE);
        assertNotNull(((Map) mapping.get("properties")).get("name"));
        assertNotNull(((Map) mapping.get("properties")).get("ft_name"));
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testAlterTableAddColumnOnPartitionedTable() throws Exception {
        execute("create table t (id int primary key, date timestamp with time zone primary key) " +
                "partitioned by (date) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");

        execute("insert into t (id, date) values (1, '2014-01-01')");
        execute("insert into t (id, date) values (10, '2015-01-01')");
        ensureYellow();
        refresh();

        execute("alter table t add name string");
        execute("select * from t");
        assertThat(Arrays.asList(response.cols()), Matchers.containsInAnyOrder("date", "id", "name"));

        GetIndexTemplatesResponse templatesResponse = client().admin().indices().getTemplates(new GetIndexTemplatesRequest(".partitioned.t.")).actionGet();
        IndexTemplateMetaData metaData = templatesResponse.getIndexTemplates().get(0);
        String mappingSource = metaData.mappings().get(DEFAULT_MAPPING_TYPE).toString();
        Map mapping = (Map) XContentFactory.xContent(mappingSource)
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingSource)
            .map().get(DEFAULT_MAPPING_TYPE);
        assertNotNull(((Map) mapping.get("properties")).get("name"));
        // template order must not be touched
        assertThat(metaData.order(), is(100));
    }

    @Test
    public void testInsertToPartitionFromQuery() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("select name from locations order by id");
        assertThat(response.rowCount(), is(13L));
        String firstName = (String) response.rows()[0][0];

        execute("create table locations_parted (" +
                " id string primary key," +
                " name string primary key," +
                " date timestamp with time zone" +
                ") clustered by(id) into 2 shards partitioned by(name) with(number_of_replicas=0)");
        ensureYellow();

        execute("insert into locations_parted (id, name, date) (select id, name, date from locations)");
        assertThat(response.rowCount(), is(13L));

        execute("refresh table locations_parted");
        execute("select name from locations_parted order by id");
        assertThat(response.rowCount(), is(13L));
        assertThat(response.rows()[0][0], is(firstName));
    }

    @Test
    public void testPartitionedTableNestedPk() throws Exception {
        execute("create table t (o object as (i int primary key, name string)) partitioned by (o['i']) with (number_of_replicas=0)");
        execute("insert into t (o) values (?)", new Object[]{Map.of("i", 1, "name", "Zaphod")});
        refresh();
        execute("select o['i'], o['name'] from t");
        assertThat(response.rows()[0][0], Matchers.is(1));
        execute("select distinct table_name, partition_ident from sys.shards where table_name = 't'");
        assertEquals("t| 04132\n", printedTable(response.rows()));
    }

    @Test
    public void testStartPartitionWithMissingTable() throws Exception {
        // ensureYellow must succeed
        String partition = ".partitioned.parted.04130";
        client().admin().indices().prepareCreate(partition).execute().actionGet();
        ensureYellow();
    }

    @Test
    public void testCreateTableWithIllegalCustomSchemaCheckedByES() {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Relation name \"AAA.t\" is invalid.");
        execute(
            "create table \"AAA\".t (" +
            "   name string," +
            "   d timestamp with time zone" +
            ") partitioned by (d) with (number_of_replicas=0)");
    }

    @Test
    public void testAlterNumberOfShards() {
        execute("create table quotes (" +
                "  id integer, " +
                "  quote string, " +
                "  date timestamp with time zone) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='0-all')");
        ensureYellow();

        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));

        execute("alter table quotes set (number_of_shards=6)");
        ensureGreen();

        templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(6));

        execute("insert into quotes (id, quote, date) values (?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L}
        );
        assertThat(response.rowCount(), is(1L));
        refresh();

        assertTrue(clusterService().state().metaData().hasAlias(getFqn("quotes")));

        execute("select number_of_replicas, number_of_shards from information_schema.tables where table_name = 'quotes'");
        assertEquals("0-all", response.rows()[0][0]);
        assertEquals(6, response.rows()[0][1]);

        execute("select number_of_shards from information_schema.table_partitions where table_name='quotes'");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(6));

        execute("alter table quotes set (number_of_shards=2)");
        ensureYellow();

        execute("insert into quotes (id, quote, date) values (?, ?, ?)",
            new Object[]{2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
        refresh();

        execute("select number_of_replicas, number_of_shards from information_schema.tables where table_name = 'quotes'");
        assertEquals("0-all", response.rows()[0][0]);
        assertEquals(2, response.rows()[0][1]);

        execute("select partition_ident, number_of_shards from information_schema.table_partitions " +
                "where table_name = 'quotes' order by number_of_shards ASC");
        assertThat(
            printedTable(response.rows()),
            is("04732cpp6ksjcc9i60o30c1g| 2\n" +
               "04732cpp6ks3ed1o60o30c1g| 6\n")
        );
        templatesResponse = client().admin().indices()
            .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(2));
    }

    @Test
    public void testGroupOnDynamicObjectColumn() throws Exception {
        execute("create table event (day timestamp with time zone primary key, data object) " +
            "clustered into 6 shards " +
            "partitioned by (day)");
        ensureYellow();
        execute("insert into event (day, data) values ('2015-01-03', {sessionid = null})");
        execute("insert into event (day, data) values ('2015-01-01', {sessionid = 'hello'})");
        execute("refresh table event");
        waitForMappingUpdateOnAll("event", "data.sessionid");
        execute("select data['sessionid'] from event group by data['sessionid'] " +
                "order by format('%s', data['sessionid'])");
        assertThat(response.rows().length, Is.is(2));
        assertThat((String) response.rows()[0][0], Is.is("hello"));
        assertThat(response.rows()[1][0], Is.is(nullValue()));
    }

    @Test
    public void testFilterOnDynamicObjectColumn() throws Exception {
        execute(
            "create table event (" +
            "   day timestamp with time zone primary key," +
            "   data object" +
            ") clustered into 6 shards " +
            "partitioned by (day)");
        ensureYellow();
        execute("insert into event (day, data) values ('2015-01-03', {sessionid = null})");
        execute("insert into event (day, data) values ('2015-01-01', {sessionid = 'hello'})");
        execute("insert into event (day, data) values ('2015-02-08', {sessionid = 'ciao'})");
        execute("refresh table event");
        waitForMappingUpdateOnAll("event", "data.sessionid");
        execute("select data['sessionid'] from event where " +
                "format('%s', data['sessionid']) = 'ciao' order by data['sessionid']");
        assertThat(response.rows().length, Is.is(1));
        assertThat((String) response.rows()[0][0], Is.is("ciao"));
    }

    @Test
    public void testOrderByDynamicObjectColumn() throws Exception {
        execute(
            "create table event (" +
            "   day timestamp with time zone primary key," +
            "   data object," +
            "   number int" +
            ") clustered into 6 shards" +
            " partitioned by (day)");
        ensureYellow();
        execute("insert into event (day, data, number) values ('2015-01-03', {sessionid = null}, 42)");
        execute("insert into event (day, data, number) values ('2015-01-01', {sessionid = 'hello'}, 42)");
        execute("insert into event (day, data, number) values ('2015-02-08', {sessionid = 'ciao'}, 42)");
        execute("refresh table event");
        waitForMappingUpdateOnAll("event", "data.sessionid");
        execute("select data['sessionid'] from event order by data['sessionid'] ASC nulls first");
        assertThat(printedTable(response.rows()), is(
            "NULL\n" +
            "ciao\n" +
            "hello\n"));

        execute("select data['sessionid'] from event order by data['sessionid'] ASC nulls last");
        assertThat(printedTable(response.rows()), is(
            "ciao\n" +
            "hello\n" +
            "NULL\n"));

        execute("select data['sessionid'] from event order by data['sessionid'] DESC nulls first");
        assertThat(printedTable(response.rows()), is(
            "NULL\n" +
            "hello\n" +
            "ciao\n"));

        execute("select data['sessionid'] from event order by data['sessionid'] DESC nulls last");
        assertThat(printedTable(response.rows()), is(
            "hello\n" +
            "ciao\n" +
            "NULL\n"));
    }

    @Test
    public void testDynamicColumnWhere() throws Exception {
        execute(
            "create table event (" +
            "   day timestamp with time zone primary key," +
            "   data object," +
            "   number int" +
            ") clustered into 6 shards " +
            "partitioned by (day)");
        ensureYellow();
        execute("insert into event (day, data, number) values ('2015-01-03', {sessionid = null}, 0)");
        execute("insert into event (day, data, number) values ('2015-01-01', {sessionid = 'hello'}, 21)");
        execute("insert into event (day, data, number) values ('2015-02-08', {sessionid = 'ciao'}, 42)");
        execute("insert into event (day, number) values ('2015-03-08', 84)");
        execute("refresh table event");
        waitForMappingUpdateOnAll("event", "data.sessionid");

        execute("select data " +
                "from event " +
                "where data['sessionid'] is null " +
                "order by number");
        assertThat(response.rowCount(), is(2L));
        assertThat(printedTable(response.rows()), is(
            "{sessionid=NULL}\n" +
            "NULL\n"));

        execute("select data " +
                "from event " +
                "where data['sessionid'] = 'ciao'");
        assertThat(response.rowCount(), is(1L));

        execute("select data " +
                "from event " +
                "where data['sessionid'] in ('hello', 'goodbye') " +
                "order by number DESC");
        assertThat(printedTable(response.rows()), is(
            "{sessionid=hello}\n"));
    }

    @Test
    public void testGroupOnDynamicColumn() throws Exception {
        execute("create table event (day timestamp with time zone primary key) " +
                "clustered into 6 shards partitioned by (day) " +
                "with (column_policy = 'dynamic') ");
        ensureYellow();
        execute("insert into event (day) values ('2015-01-03')");
        execute("insert into event (day, sessionid) values ('2015-01-01', 'hello')");
        execute("refresh table event");

        waitForMappingUpdateOnAll("event", "sessionid");
        execute("select sessionid from event group by sessionid order by sessionid");
        assertThat(response.rows().length, Is.is(2));
        assertThat((String) response.rows()[0][0], Is.is("hello"));
        assertThat(response.rows()[1][0], Is.is(nullValue()));
    }

    @Test
    public void testFetchPartitionedTable() {
        // clear jobs logs
        execute("set global stats.enabled = false");
        execute("set global stats.enabled = true");

        execute("create table fetch_partition_test (name string, p string) partitioned by (p) with (number_of_replicas=0)");
        ensureYellow();
        Object[][] bulkArgs = new Object[3][];
        for (int i = 0; i < 3; i++) {
            bulkArgs[i] = new Object[]{"Marvin", i};
        }
        execute("insert into fetch_partition_test (name, p) values (?, ?)", bulkArgs);
        execute("refresh table fetch_partition_test");
        execute("select count(*) from fetch_partition_test");
        assertThat(response.rows()[0][0], is(3L));
        execute("select count(*), job_id, arbitrary(name) from sys.operations_log where name='fetch' group by 2");
        assertThat(response.rowCount(), is(lessThanOrEqualTo(1L)));
    }

    @Test
    public void testDeleteOrphanedPartitions() throws Throwable {
        execute("create table foo (name string, p string) partitioned by (p) with (number_of_replicas=0, refresh_interval = 0)");
        ensureYellow();
        execute("insert into foo (name, p) values (?, ?)", new Object[]{"Marvin", 1});
        execute("refresh table foo");

        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "foo");
        client().admin().indices().prepareDeleteTemplate(templateName).execute().actionGet();
        waitNoPendingTasksOnAll();
        execute("select * from sys.shards where table_name = 'foo'");
        assertThat(response.rowCount(), Matchers.greaterThan(0L));
        execute("drop table foo");

        execute("select * from sys.shards where table_name = 'foo'");
        assertThat(response.rowCount(), CoreMatchers.is(0L));
    }

    @Test
    public void testAlterTableAfterDeleteDoesNotAttemptToAlterDeletedPartitions() throws Exception {
        execute("create table t (name string, p string) partitioned by (p)");
        ensureYellow();

        execute("insert into t (name, p) values (?, ?)", new Object[][]{
            new Object[]{"Arthur", "1"},
            new Object[]{"Arthur", "2"},
            new Object[]{"Arthur", "3"},
            new Object[]{"Arthur", "4"},
            new Object[]{"Arthur", "5"},
        });
        execute("refresh table t");
        execute("delete from t");
        // used to throw IndexNotFoundException if the new cluster state after the delete wasn't propagated to all nodes
        // (on about 2 runs in 100 iterations)
        execute("alter table t set (number_of_replicas = 0)");
    }

    @Test
    public void testPartitionedColumnIsNotIn_Raw() throws Exception {
        execute("create table t (p string primary key, v string) " +
                "partitioned by (p) " +
                "with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (p, v) values ('a', 'Marvin')");
        execute("refresh table t");
        execute("select _raw from t");
        assertThat(((String) response.rows()[0][0]), is("{\"v\":\"Marvin\"}"));
    }

    @Test
    public void testMatchPredicateOnPartitionedTableWithPKColumn() throws Throwable {
        execute("create table foo (id integer primary key, name string INDEX using fulltext) partitioned by (id)");
        ensureYellow();
        execute("insert into foo (id, name) values (?, ?)", new Object[]{1, "Marvin Other Name"});
        execute("insert into foo (id, name) values (?, ?)", new Object[]{2, "Ford Yet Another Name"});
        execute("refresh table foo");

        execute("select id from foo where match(name, 'Ford')");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(2));
    }

    @Test
    public void testMatchPredicateOnPartitionedTableWithoutPKColumn() throws Throwable {
        execute("create table foo (id integer, name string INDEX using fulltext) partitioned by (id)");
        ensureYellow();
        execute("insert into foo (id, name) values (?, ?)", new Object[]{1, "Marvin Other Name"});
        execute("insert into foo (id, name) values (?, ?)", new Object[]{2, "Ford Yet Another Name"});
        execute("refresh table foo");

        execute("select id from foo where match(name, 'Marvin')");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(1));
    }

    @Test
    public void testAlterSettingsEmptyPartitionedTableDoNotAffectAllTables() throws Exception {
        execute("create table tweets (id string primary key)" +
                " with (number_of_replicas='0')");
        execute("create table device_event (id long, reseller_id long, date_partition string, value float," +
                " primary key (id, reseller_id, date_partition))" +
                " partitioned by (date_partition)" +
                " with (number_of_replicas='0')");
        ensureYellow();

        execute("alter table device_event SET (number_of_replicas='3')");

        execute("select table_name, number_of_replicas from information_schema.tables where table_schema = ? order by table_name",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat((String) response.rows()[0][1], is("3"));
        assertThat((String) response.rows()[1][1], is("0"));
    }

    @Test
    public void testSelectByIdEmptyPartitionedTable() {
        execute("create table test (id integer, entity integer, primary key(id, entity))" +
                " partitioned by (entity) with (number_of_replicas=0)");
        ensureYellow();
        execute("select * from test where entity = 0 and id = 0");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testSelectByMultiIdEmptyPartitionedTable() {
        execute("create table test (id integer, entity integer, primary key(id, entity))" +
                " partitioned by (entity) with (number_of_replicas=0)");
        ensureYellow();
        execute("select * from test where entity = 0 and (id = 0 or id = 1)");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testScalarEvaluatesInErrorOnPartitionedTable() throws Exception {
        execute("create table t1 (id int) partitioned by (id) with (number_of_replicas=0)");
        ensureYellow();
        // we need at least 1 row/partition, otherwise the table is empty and no evaluation occurs
        execute("insert into t1 (id) values (1)");
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(" / by zero");
        execute("select id/0 from t1");
    }

    @Test
    public void testSelectPartitionValueFromInformationSchema() throws Exception {
        execute("create table t1 (p int, obj object as (p int)) " +
                "partitioned by (p, obj['p']) " +
                "clustered into 1 shards");
        execute("insert into t1 (p, obj) values (1, {p=10})");

        execute("select values['p'], values['obj[''p'']'] from information_schema.table_partitions");
        assertThat(printedTable(response.rows()), is("1| 10\n"));
    }

    @Test
    public void testRefreshIgnoresClosedPartitions() {
        execute("create table t (x int, p int) " +
                "partitioned by (p) clustered into 1 shards with (number_of_replicas = 0, refresh_interval = 0)");
        execute("insert into t (x, p) values (1, 1), (2, 2)");
        execute("alter table t partition (p = 2) close");
        assertThat(execute("refresh table t").rowCount(), is(1L));
        assertThat(
            printedTable(execute("select * from t").rows()),
            is("1| 1\n")
        );
    }

    @Test
    public void test_refresh_not_existing_partition() {
        execute("CREATE TABLE doc.parted (x TEXT) PARTITIONED BY (x)");
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            "No partition for table 'doc.parted' with ident '048mgp34ed3ksii8ad3kcha6bb1po' exists");
        execute("REFRESH TABLE doc.parted PARTITION (x = 'hddsGNJHSGFEFZÜ')");
    }

    @Test
    public void test_refresh_partition_in_non_partitioned_table() {
        execute("CREATE TABLE doc.not_parted (x TEXT)");
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("table 'doc.not_parted' is not partitioned");
        execute("REFRESH TABLE doc.not_parted PARTITION (x = 'n')");
    }

    @Test
    public void test_partition_filter_and_column_filter_are_both_applied() {
        execute("create table t (p string primary key, v string) " +
                "partitioned by (p) " +
                "with (number_of_replicas = 0)");
        execute("insert into t (p, v) values ('a', 'Marvin')");
        execute("insert into t (p, v) values ('b', 'Marvin')");
        execute("refresh table t");

        execute("select * from t where p='a' and v='Marvin'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("a| Marvin\n"));
    }

    @Test
    public void test_partition_filter_on_table_with_generated_column() {
        execute(
            "create table t (" +
            "   t timestamp with time zone, " +
            "   day timestamp with time zone GENERATED ALWAYS AS date_trunc('day', t)" +
            ") partitioned by (day)");
        execute("insert into t(t) values ('2018-03-28T12:00:00+07:00');");
        execute("insert into t(t) values ('2018-01-01T12:00:00+07:00');");
        execute("refresh table t");

        execute("select count(*) from t where t > 1000");
        assertThat(TestingHelpers.printedTable(response.rows()), is("2\n"));
    }

    @Test
    public void testOrderingOnPartitionColumn() {
        execute("create table t (x int, p int) partitioned by (p) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into t (p, x) values (1, 1), (1, 2), (2, 1)");
        execute("refresh table t");
        assertThat(
            printedTable(execute("select p, x from t order by p desc, x asc").rows()),
            is("2| 1\n" +
               "1| 1\n" +
               "1| 2\n")
        );
    }

    @Test
    public void testDisableWriteOnSinglePartition() {
        execute("create table my_table (par int, content string) " +
                "clustered into 5 shards " +
                "partitioned by (par)");
        execute("insert into my_table (par, content) values (1, 'content1'), " +
                "(1, 'content2'), " +
                "(2, 'content3'), " +
                "(2, 'content4'), " +
                "(2, 'content5'), " +
                "(3, 'content6')");

        ensureGreen();
        execute("alter table my_table partition (par=1) set (\"blocks.write\"=true)");

        // update is expected to be executed without exception since this partition has no write block
        execute("update my_table set content=\'content42\' where par=2");
        refresh();
        // verifying update
        execute("select content from my_table where par=2");
        assertThat(response.rowCount(), Matchers.is(3L));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("blocked by: [FORBIDDEN/8/index write (api)]");

        // trying to perform an update on a partition with a write block
        execute("update my_table set content=\'content42\' where par=1");
    }

    @Test
    public void testMultipleWritesWhenOnePartitionIsReadOnly() {
        execute("create table my_table (par int, content string) " +
                "clustered into 5 shards " +
                "partitioned by (par)");
        execute("insert into my_table (par, content) values " +
                "(1, 'content2'), " +
                "(2, 'content3')");

        ensureGreen();
        execute("alter table my_table partition (par=1) set (\"blocks.write\"=true)");
        try {
            execute("insert into my_table (par, content) values (2, 'content42'), " +
                    "(2, 'content42'), " +
                    "(1, 'content2'), " +
                    "(3, 'content6')");
            fail("expected to throw an \"blocked\" exception");
        } catch (SQLActionException e) {
            assertThat(e.getMessage(), containsString("blocked by: [FORBIDDEN/8/index write (api)];"));
        }
        refresh();
        execute("select * from my_table");
        assertThat(response.rowCount(), Matchers.is(both(greaterThanOrEqualTo(2L)).and(lessThanOrEqualTo(5L))));
        //cleaning up
        execute("alter table my_table partition (par=1) set (\"blocks.write\"=false)");
    }

    @Test
    public void testCurrentVersionsAreSetOnPartitionCreation() throws Exception {
        execute("create table doc.p1 (id int, p int) partitioned by (p)");

        execute("insert into doc.p1 (id, p) values (1, 2)");
        execute("select version['created'] from information_schema.table_partitions where table_name='p1'");

        assertThat(response.rows()[0][0], is(Version.CURRENT.externalNumber()));
    }

    @Test
    public void test_where_clause_that_could_match_on_null_partition_filters_correct_records() {
        execute("create table t (id int, p int) clustered into 1 shards partitioned by (p)");
        execute("insert into t (id, p) values (?, ?)", $$(
            $(1, null),
            $(2, 1),
            $(3, 1),
            $(4, 2)));
        execute("refresh table t");

        execute("select id from t where p = 1 or id = 4 order by 1");
        assertThat(
            printedTable(response.rows()),
            is("2\n" + // match on p = 1
               "3\n" + // match on p = 1
               "4\n")  // match on id = 4
        );
    }

    @Test
    public void test_insert_from_subquery_and_values_into_same_partition_for_object_ts_field_as_partition_key() {
        execute(
            "CREATE TABLE test (" +
            "   id INT," +
            "   metadata OBJECT AS (date TIMESTAMP WITHOUT TIME ZONE)" +
            ") CLUSTERED INTO 1 SHARDS " +
            "PARTITIONED BY (metadata['date'])");

        execute("INSERT INTO test (id, metadata) (SELECT ?, ?)", new Object[]{1, Map.of("date", "2014-05-28")});
        execute("INSERT INTO test (id, metadata) VALUES (?, ?)", new Object[]{2, Map.of("date", "2014-05-28")});
        refresh();

        execute(
            "SELECT table_name, partition_ident, values " +
            "FROM information_schema.table_partitions " +
            "ORDER BY table_name, partition_ident");
        assertThat(
            printedTable(response.rows()),
            is("test| 04732d1g64p36d9i60o30c1g| {metadata['date']=1401235200000}\n"));
        assertThat(printedTable(execute("SELECT count(*) FROM test").rows()), is("2\n"));
    }

    @Test
    public void test_nested_partition_column_is_included_when_selecting_the_object_but_not_in_the_source() {
        execute("create table tbl (pk object as (id text, part text), primary key (pk['id'], pk['part'])) " +
                "partitioned by (pk['part'])");
        execute("insert into tbl (pk) values ({id='1', part='x'})");
        execute("insert into tbl (pk) (select {id='2', part='x'})");
        execute("refresh table tbl");
        execute("select _raw, pk, pk['id'], pk['part'] from tbl order by pk['id'] asc");
        assertThat(
            printedTable(response.rows()),
            is("{\"pk\":{\"id\":\"1\"}}| {id=1, part=x}| 1| x\n" +
               "{\"pk\":{\"id\":\"2\"}}| {id=2, part=x}| 2| x\n")
        );

        execute("SELECT _raw, pk, pk['id'], pk['part'] FROM tbl " +
                " WHERE (pk['id'] = 1 AND pk['part'] = 'x') " +
                " OR    (pk['id'] = 2 AND pk['part'] = 'x') " +
                " ORDER BY pk['id'] ASC");
        assertThat(
            printedTable(response.rows()),
            is("{\"pk\":{\"id\":\"1\"}}| {id=1, part=x}| 1| x\n" +
               "{\"pk\":{\"id\":\"2\"}}| {id=2, part=x}| 2| x\n")
        );
    }
}
