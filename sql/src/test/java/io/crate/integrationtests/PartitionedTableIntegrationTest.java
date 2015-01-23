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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.metadata.PartitionName;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.*;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class PartitionedTableIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Setup setup = new Setup(sqlExecutor);

    private String copyFilePath = getClass().getResource("/essetup/data/copy").getPath();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCopyFromIntoPartitionedTableWithPARTITIONKeyword() throws Exception {
        execute("create table quotes (" +
                "id integer primary key," +
                "date timestamp primary key," +
                "quote string index using fulltext" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureGreen();
        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");
        execute("copy quotes partition (date=1400507539938) from ?", new Object[]{filePath});
        refresh();
        execute("select id, date, quote from quotes order by id asc");
        assertEquals(3L, response.rowCount());
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Long) response.rows()[0][1], is(1400507539938L));
        assertThat((String) response.rows()[0][2], is("Don't pañic."));

        execute("select count(*) from information_schema.table_partitions where table_name = 'quotes'");
        assertThat((Long) response.rows()[0][0], is(1L));

        execute("copy quotes partition (date=1800507539938) from ?", new Object[]{filePath});
        refresh();

        execute("select partition_ident from information_schema.table_partitions " +
                "where table_name = 'quotes' " +
                "order by partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("04732d1g60qj0dpl6csjicpo"));
        assertThat((String) response.rows()[1][0], is("04732e1g60qj0dpl6csjicpo"));
    }

    @Test
    public void testCopyFromIntoPartitionedTable() throws Exception {
        execute("create table quotes (" +
                "  id integer primary key, " +
                "  quote string index using fulltext" +
                ") partitioned by (id)");
        ensureGreen();

        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");
        execute("copy quotes from ?", new Object[]{filePath});
        // 2 nodes on same machine resulting in double affected rows
        assertEquals(6L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();
        ensureGreen();

        for (String id : ImmutableList.of("1", "2", "3")) {
            String partitionName = new PartitionName("quotes", ImmutableList.of(new BytesRef(id))).stringValue();
            assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                    .getState().metaData().indices().get(partitionName));
            assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                    .getState().metaData().indices().get(partitionName).aliases().get("quotes"));
        }

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));
    }

    @Test
    public void testCreatePartitionedTableAndQueryMeta() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();

        execute("select * from information_schema.tables where schema_name='doc' order by table_name");
        assertEquals(1L, response.rowCount());
        assertEquals("quotes", response.rows()[0][1]);

        execute("select * from information_schema.columns where table_name='quotes' order by ordinal_position");
        assertEquals(3L, response.rowCount());
        assertEquals("id", response.rows()[0][2]);
        assertEquals("quote", response.rows()[1][2]);
        assertEquals("timestamp", response.rows()[2][2]);

    }

    @Test
    public void testInsertPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        String templateName = PartitionName.templateName(null, "parted");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        assertThat(templatesResponse.getIndexTemplates().get(0).template(),
                is(templateName + "*"));
        assertThat(templatesResponse.getIndexTemplates().get(0).name(),
                is(templateName));
        assertTrue(templatesResponse.getIndexTemplates().get(0).aliases().containsKey("parted"));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{1, "Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();

        assertTrue(clusterService().state().metaData().aliases().containsKey("parted"));

        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef(String.valueOf(13959981214861L)))
        ).stringValue();
        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData();
        assertNotNull(metaData.indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );

        execute("select id, name, date from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((String) response.rows()[0][1], is("Ford"));
        assertThat((Long) response.rows()[0][2], is(13959981214861L));
    }

    private void validateInsertPartitionedTable() {
        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef(String.valueOf(13959981214861L)))
        ).stringValue();
        assertTrue(cluster().clusterService().state().metaData().hasIndex(partitionName));
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );

        partitionName = new PartitionName("parted", Arrays.asList(new BytesRef(String.valueOf(0L)))).stringValue();
        assertTrue(cluster().clusterService().state().metaData().hasIndex(partitionName));
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );

        List<BytesRef> nullList = new ArrayList<>();
        nullList.add(null);
        partitionName = new PartitionName("parted", nullList).stringValue();
        assertTrue(cluster().clusterService().state().metaData().hasIndex(partitionName));
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );
    }

    @Test
    public void testMultiValueInsertPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Ford", 13959981214861L,
                        2, "Trillian", 0L,
                        3, "Zaphod", null
                });
        assertThat(response.rowCount(), is(3L));
        ensureGreen();
        refresh();

        validateInsertPartitionedTable();
    }

    @Test
    public void testBulkInsertPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("insert into parted (id, name, date) values (?, ?, ?)", new Object[][] {
                new Object[]{ 1, "Ford", 13959981214861L },
                new Object[]{ 2, "Trillian", 0L },
                new Object[]{ 3, "Zaphod", null }
        });
        ensureGreen();
        refresh();

        validateInsertPartitionedTable();
    }

    @Test
    public void testInsertPartitionedTableOnlyPartitionedColumns() throws Exception {
        execute("create table parted (name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureGreen();

        execute("insert into parted (name, date) values (?, ?)",
                new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef("Ford"), new BytesRef(String.valueOf(13959981214861L)))
        ).stringValue();
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );
        execute("select * from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), arrayContaining("date", "name"));
        assertThat((Long) response.rows()[0][0], is(13959981214861L));
        assertThat((String) response.rows()[0][1], is("Ford"));
    }

    @Test
    public void testInsertPartitionedTableOnlyPartitionedColumnsAlreadyExsists() throws Exception {
        execute("create table parted (name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureGreen();

        execute("insert into parted (name, date) values (?, ?)",
                new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        execute("insert into parted (name, date) values (?, ?)",
                new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();

        execute("select name, date from parted");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("Ford"));
        assertThat((String) response.rows()[1][0], is("Ford"));

        assertThat((Long) response.rows()[0][1], is(13959981214861L));
        assertThat((Long) response.rows()[1][1], is(13959981214861L));
    }

    @Test
    public void testInsertPartitionedTablePrimaryKeysDuplicate() throws Exception {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp," +
                "  primary key (id, name)" +
                ") partitioned by (id, name)");
        ensureGreen();
        Long dateValue = System.currentTimeMillis();
        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{42, "Zaphod", dateValue});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("A document with the same primary key exists already");

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{42, "Zaphod", 0L});
    }

    @Test
    public void testInsertPartitionedTableSomePartitionedColumns() throws Exception {
        // insert only some partitioned column values
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureGreen();

        execute("insert into parted (id, name) values (?, ?)",
                new Object[]{1, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef("Trillian"), null)).stringValue();
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));

        execute("select id, name, date from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((String) response.rows()[0][1], is("Trillian"));
        assertNull(response.rows()[0][2]);
    }

    @Test
    public void testInsertPartitionedTableReversedPartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureGreen();

        Long dateValue = System.currentTimeMillis();
        execute("insert into parted (id, date, name) values (?, ?, ?)",
                new Object[]{1, dateValue, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        refresh();
        String partitionName = new PartitionName("parted",
                Arrays.asList(new BytesRef("Trillian"), new BytesRef(dateValue.toString()))).stringValue();
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
    }

    @Test
    public void testSelectFromPartitionedTableWhereClause() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("select id, quote from quotes where (timestamp = 1395961200000 or timestamp = 1395874800000) and id = 1");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testSelectCountFromPartitionedTableWhereClause() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("select count(*) from quotes where (timestamp = 1395961200000 or timestamp = 1395874800000)");
        assertEquals(1L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);

        execute("select count(*) from quotes where timestamp = 1");
        assertEquals(0L, response.rows()[0][0]);
    }

    @Test
    public void testSelectFromPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();
        execute("select id, quote, timestamp as ts, timestamp from quotes where timestamp > 1395874800000");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(2));
        assertThat((String) response.rows()[0][1], is("Time is an illusion. Lunchtime doubly so"));
        assertThat((Long) response.rows()[0][2], is(1395961200000L));
    }

    @Test
    public void testSelectPrimaryKeyFromPartitionedTable() throws Exception {
        execute("create table stuff (" +
                "  id integer primary key, " +
                "  type byte primary key," +
                "  content string) " +
                "partitioned by(type) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{1, 127, "Don't panic"});
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{2, 126, "Time is an illusion. Lunchtime doubly so"});
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{3, 126, "Now panic"});
        ensureGreen();
        refresh();

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
    public void testUpdatePartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
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
    public void testUpdatePartitionedUnknownPartition() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp, o object) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote='now panic' where timestamp = ?", new Object[]{ 1395874800123L });
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdatePartitionedUnknownColumn() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp, o object(ignored)) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote='now panic' where o['timestamp'] = ?", new Object[]{ 1395874800123L });
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdatePartitionedUnknownColumnKnownValue() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote='now panic' where timestamp = ? and quote=?",
                new Object[]{ 1395874800123L, "Don't panic" });
        assertThat(response.rowCount(), is(0L));
        refresh();

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testUpdateUnknownColumnKnownValueAndConjunction() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Don't panic", 1395961200000L});
        ensureGreen();
        refresh();

        execute("update quotes set quote='now panic' where not timestamp = ? and quote=?",
                new Object[]{ 1395874800000L, "Don't panic" });
        refresh();
        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount(), is(1L));
    }


    @Test
    public void testDeleteFromPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        ensureGreen();
        refresh();

        execute("delete from quotes where timestamp = 1395874800000 and id = 1");
        assertEquals(-1, response.rowCount());
        refresh();

        execute("select id, quote from quotes where timestamp = 1395874800000");
        assertEquals(0L, response.rowCount());

        execute("select id, quote from quotes");
        assertEquals(2L, response.rowCount());

        execute("delete from quotes");
        assertEquals(-1, response.rowCount());
        refresh();

        execute("select id, quote from quotes");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testDeleteFromPartitionedTableUnknownPartition() throws Exception {
        this.setup.partitionTableSetup();
        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='parted' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat((String)response.rows()[0][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1388534400000"))).ident()));
        assertThat((String)response.rows()[1][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1391212800000"))).ident()));

        execute("delete from parted where date = '2014-03-01'");
        refresh();
        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='parted' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableWrongPartitionedColumn() throws Exception {
        this.setup.partitionTableSetup();

        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='parted' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat((String)response.rows()[0][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1388534400000"))).ident()));
        assertThat((String)response.rows()[1][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1391212800000"))).ident()));

        execute("delete from parted where o['dat'] = '2014-03-01'");
        refresh();
        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='parted' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableDeleteByQuery() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{4, "Now panic", 1395874800000L});
        ensureGreen();
        refresh();

        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='quotes' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(response.rowCount(), is(3L));
        assertThat((String)response.rows()[0][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1395874800000"))).ident()));
        assertThat((String)response.rows()[1][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1395961200000"))).ident()));
        assertThat((String)response.rows()[2][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1396303200000"))).ident()));

        execute("delete from quotes where quote = 'Don''t panic'");
        refresh();

        execute("select * from quotes where quote = 'Don''t panic'");
        assertThat(this.response.rowCount(), is(0L));

        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                "where table_name='quotes' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(newResponse.rows(), is(response.rows()));
    }

    @Test
    public void testDeleteFromPartitionedTableDeleteByPartitionAndByQuery() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp, o object(ignored)) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "I'd far rather be happy than right any day", 1396303200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{4, "Now panic", 1395874800000L});
        ensureGreen();
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
    public void testDeleteFromPartitionedTableDeleteByPartitionAndQueryWithConjunction() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Don't panic", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "Don't panic", 1396303200000L});
        ensureGreen();
        refresh();

        execute("delete from quotes where not timestamp=? and quote=?", new Object[]{1396303200000L, "Don't panic"});
        refresh();
        execute("select * from quotes");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testGlobalAggregatePartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
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
        ensureGreen();
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
        ensureGreen();
        refresh();

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{2, "Arthur", 1001L});
        ensureGreen();
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
    public void testGroupByPartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("select date, count(*) from parted group by date");
        assertThat(response.rowCount(), is(0L));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{0, "Trillian", 100L});
        ensureGreen();
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
        ensureGreen();
        refresh();

        execute("select date, count(*) from parted group by date order by count(*) desc");
        assertThat(response.rowCount(), is(2L));
        assertNull(response.rows()[0][0]);
        assertThat((Long) response.rows()[0][1], is(2L));
        assertThat((Long) response.rows()[1][0], is(100L));
        assertThat((Long) response.rows()[1][1], is(1L));
    }

    @Test
    public void testGroupByPartitionedColumnWhereClause() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("select date, count(*) from parted where date > 0 group by date");
        assertThat(response.rowCount(), is(0L));

        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{0, "Trillian", 100L});
        ensureGreen();
        refresh();

        execute("select date, count(*) from parted where date > 0 group by date");
        assertThat(response.rowCount(), is(1L));

        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Arthur", 0L,
                        2, "Ford", 2437646253L
                }
        );
        ensureGreen();
        refresh();

        execute("select date, count(*) from parted where date > 100 group by date");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2437646253L));
        assertThat((Long) response.rows()[0][1], is(1L));
    }

    @Test
    public void testGlobalAggregateWhereClause() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureGreen();
        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted where date > 0");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(0L));
        assertThat((Long) response.rows()[0][1], is(0L));
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
        ensureGreen();
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
                "  date timestamp" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, date) values(?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureGreen();
        refresh();

        execute("drop table quotes");
        assertEquals(1L, response.rowCount());

        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices()
                .prepareGetTemplates(PartitionName.templateName(null, "quotes")).execute().get();
        assertThat(getIndexTemplatesResponse.getIndexTemplates().size(), is(0));

        assertThat(cluster().clusterService().state().metaData().indices().size(), is(0));

        AliasesExistResponse aliasesExistResponse = client().admin().indices()
                .prepareAliasesExist("quotes").execute().get();
        assertFalse(aliasesExistResponse.exists());
    }

    @Test
    public void testPartitionedTableSelectById() throws Exception {
        execute("create table quotes (id integer, quote string, num double, primary key (id, num)) partitioned by (num)");
        ensureGreen();
        execute("insert into quotes (id, quote, num) values (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Don't panic", 4.0d,
                        2, "Time is an illusion. Lunchtime doubly so", -4.0d
                });
        ensureGreen();
        refresh();
        execute("select * from quotes where id = 1 and num = 4");
        assertThat(response.rowCount(), is(1L));
        assertThat(Joiner.on(", ").join(response.cols()), is("id, num, quote"));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Double) response.rows()[0][1], is(4.0d));
        assertThat((String) response.rows()[0][2], is("Don't panic"));
    }

    @Test
    public void testInsertDynamicToPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp," +
                "author object(dynamic) as (name string)) " +
                "partitioned by(date) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, date, author) values(?, ?, ?, ?), (?, ?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        new HashMap<String, Object>() {{
                            put("name", "Douglas");
                        }},
                        2, "Time is an illusion. Lunchtime doubly so", 1395961200000L,
                        new HashMap<String, Object>() {{
                            put("name", "Ford");
                        }}});
        ensureGreen();
        refresh();

        execute("select * from information_schema.columns where table_name = 'quotes'");
        assertEquals(5L, response.rowCount());

        execute("insert into quotes (id, quote, date, author) values(?, ?, ?, ?)",
                new Object[]{3, "I'd far rather be happy than right any day", 1395874800000L,
                        new HashMap<String, Object>() {{
                            put("name", "Douglas");
                            put("surname", "Adams");
                        }}});
        ensureGreen();
        refresh();

        execute("select * from information_schema.columns where table_name = 'quotes'");
        assertEquals(6L, response.rowCount());

        execute("select author['surname'] from quotes order by id");
        assertEquals(3L, response.rowCount());
        assertNull(response.rows()[0][0]);
        assertNull(response.rows()[1][0]);
        assertEquals("Adams", response.rows()[2][0]);
    }

    @Test
    public void testPartitionedTableAllConstraintsRoundTrip() throws Exception {
        execute("create table quotes (id integer primary key, quote string, " +
                "date timestamp primary key, user_id string primary key) " +
                "partitioned by(date, user_id) clustered by (id) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L, "Arthur"});
        assertEquals(1L, response.rowCount());
        execute("insert into quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L, "Ford"});
        assertEquals(1L, response.rowCount());
        ensureGreen();
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
    public void testPartitionedTableSchemaAllConstraintsRoundTrip() throws Exception {
        execute("create table my_schema.quotes (id integer primary key, quote string, " +
                "date timestamp primary key, user_id string primary key) " +
                "partitioned by(date, user_id) clustered by (id) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into my_schema.quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L, "Arthur"});
        assertEquals(1L, response.rowCount());
        execute("insert into my_schema.quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L, "Ford"});
        assertEquals(1L, response.rowCount());
        ensureGreen();
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
        execute("create table foo (" +
                "   id int primary key," +
                "   date timestamp primary key" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into foo (id, date, foo) values (1, '2014-01-01', 'foo')");
        execute("insert into foo (id, date, bar) values (2, '2014-02-01', 'bar')");
        refresh();
        ensureGreen();

        // schema updates are async and cannot reliably be forced
        int retry = 0;
        while (retry < 100) {
            execute("select * from foo");
            if (response.cols().length == 4) { // at some point both foo and bar columns must be present
                break;
            }
            Thread.sleep(100);
            retry++;
        }
        assertTrue(retry < 100);
    }

    @Test
    public void testPartitionedTableNestedAllConstraintsRoundTrip() throws Exception {
        execute("create table quotes (" +
                "id integer, " +
                "quote string, " +
                "created object as(" +
                "  date timestamp, " +
                "  user_id string)" +
                ") partitioned by(created['date']) clustered by (id) with (number_of_replicas=0)");
        assertThat(response.rowCount(), is(1L));
        ensureGreen();
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
                new Object[]{1, "Don't panic", new MapBuilder<String, Object>().put("date", 1395874800000L).put("user_id", "Arthur").map()});
        assertEquals(1L, response.rowCount());
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", new MapBuilder<String, Object>().put("date", 1395961200000L).put("user_id", "Ford").map()});
        assertEquals(1L, response.rowCount());
        ensureGreen();
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
        assertEquals(-1L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(1L, response.rowCount());

        execute("drop table quotes");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testAlterPartitionTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='0-all')");
        ensureGreen();
        assertThat(response.rowCount(), is(1L));

        String templateName = PartitionName.templateName(null, "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));

        execute("alter table quotes set (number_of_replicas=0)");
        ensureGreen();

        templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(0));
        assertThat(templateSettings.getAsBoolean(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, true), is(false));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureGreen();
        refresh();

        assertTrue(clusterService().state().metaData().aliases().containsKey("quotes"));

        execute("select number_of_replicas, number_of_shards from information_schema.tables where table_name = 'quotes'");
        assertEquals("0", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);

        execute("alter table quotes set (number_of_replicas='1-all')");
        ensureGreen();

        execute("select number_of_replicas from information_schema.tables where table_name = 'quotes'");
        assertEquals("1-all", response.rows()[0][0]);

        templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1), is(0));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("1-all"));

        List<String> partitions = ImmutableList.of(
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395961200000"))).stringValue()
        );
        Thread.sleep(1000);
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(
                partitions.get(0), partitions.get(1)
        ).execute().get();

        for (String index : partitions) {
            assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("1"));
            assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("1-all"));
        }
    }

    @Test
    public void testAlterTableResetEmptyPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='1-all')");
        ensureGreen();
        assertThat(response.rowCount(), is(1L));

        String templateName = PartitionName.templateName(null, "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("1-all"));

        execute("alter table quotes reset (number_of_replicas)");
        ensureGreen();

        templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));

    }

    @Test
    public void testAlterTableResetPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='1-all')");
        ensureGreen();
        assertThat(response.rowCount(), is(1L));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureGreen();
        refresh();

        execute("alter table quotes reset (number_of_replicas)");
        ensureGreen();

        String templateName = PartitionName.templateName(null, "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));

        List<String> partitions = ImmutableList.of(
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395961200000"))).stringValue()
        );
        Thread.sleep(1000);
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(
                partitions.get(0), partitions.get(1)
        ).execute().get();

        for (String index : partitions) {
            assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("1"));
            assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));
        }

    }

    @Test
    public void testAlterPartitionedTablePartition() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas=0)");
        ensureGreen();
        assertThat(response.rowCount(), is(1L));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureGreen();
        refresh();

        execute("alter table quotes partition (date=1395874800000) set (number_of_replicas=1)");
        ensureGreen();
        List<String> partitions = ImmutableList.of(
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395961200000"))).stringValue()
        );

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(
                partitions.get(0), partitions.get(1)
        ).execute().get();
        assertThat(settingsResponse.getSetting(partitions.get(0), IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("1"));
        assertThat(settingsResponse.getSetting(partitions.get(1), IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("0"));

        String templateName = PartitionName.templateName(null, "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(0));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("false"));

    }

    @Test
    public void testRefreshPartitionedTableAllPartitions() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) with (refresh_interval=0)");
        ensureGreen();

        execute("refresh table parted");
        assertThat(response.rowCount(), is(-1L));

        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01'), " +
                "(2, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));
        ensureGreen();

        // cannot tell what rows are visible
        // could be none, could be all
        execute("select count(*) from parted");
        // cannot exactly tell which rows are visible
        assertThat((Long) response.rows()[0][0], lessThanOrEqualTo(2L));

        execute("refresh table parted");
        assertThat(response.rowCount(), is(-1L));

        // assert that all is available after refresh
        execute("select count(*) from parted");
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testRefreshEmptyPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) with (refresh_interval=0)");
        ensureGreen();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("No partition for table 'parted' with ident '04130' exists");
        execute("refresh table parted partition(date=0)");
    }

    @Test
    public void testRefreshPartitionedTableSinglePartitions() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) " +
                "with (number_of_replicas=0, refresh_interval=-1)");
        ensureGreen();
        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01')," +
                "(2, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));

        ensureGreen();
        execute("refresh table parted");
        assertThat(response.rowCount(), is(-1L));

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
        assertThat(response.rowCount(), is(-1L));

        // assert all partition rows are available after refresh
        execute("select * from parted where date='1970-01-01'");
        assertThat(response.rowCount(), is(2L));

        execute("refresh table parted PARTITION (date='1970-01-07')");
        assertThat(response.rowCount(), is(-1L));

        // assert all partition rows are available after refresh
        execute("select * from parted where date='1970-01-07'");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testCountPartitionedTable() throws Exception {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureGreen();

        execute("select count(*) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(0L));

        execute("insert into parted (id, name, date) values (1, 'Trillian', '1970-01-01'), (2, 'Ford', '2010-01-01')");
        ensureGreen();
        execute("refresh table parted");

        execute("select count(*) from parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testAlterTableAddColumnOnPartitionedTableWithoutPartitions() throws Exception {
        execute("create table t (id int primary key, date timestamp primary key) " +
                "partitioned by (date) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureGreen();

        execute("alter table t add column name string");
        execute("alter table t add column ft_name string index using fulltext");
        ensureGreen();

        execute("select * from t");
        assertThat(Arrays.asList(response.cols()), Matchers.containsInAnyOrder("date", "ft_name", "id", "name"));

        GetIndexTemplatesResponse templatesResponse = client().admin().indices().getTemplates(new GetIndexTemplatesRequest(".partitioned.t.")).actionGet();
        IndexTemplateMetaData metaData = templatesResponse.getIndexTemplates().get(0);
        String mappingSource = metaData.mappings().get(Constants.DEFAULT_MAPPING_TYPE).toString();
        Map mapping = (Map) XContentFactory.xContent(mappingSource)
                .createParser(mappingSource)
                .mapAndClose()
                .get(Constants.DEFAULT_MAPPING_TYPE);
        assertNotNull(((Map) mapping.get("properties")).get("name"));
        assertNotNull(((Map) mapping.get("properties")).get("ft_name"));
    }

    @Test
    public void testAlterTableAddColumnOnPartitionedTable() throws Exception {
        execute("create table t (id int primary key, date timestamp primary key) " +
                "partitioned by (date) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");

        execute("insert into t (id, date) values (1, '2014-01-01')");
        execute("insert into t (id, date) values (10, '2015-01-01')");
        ensureGreen();
        refresh();

        execute("alter table t add name string");
        execute("select * from t");
        assertThat(Arrays.asList(response.cols()), Matchers.containsInAnyOrder("date", "id", "name"));

        GetIndexTemplatesResponse templatesResponse = client().admin().indices().getTemplates(new GetIndexTemplatesRequest(".partitioned.t.")).actionGet();
        IndexTemplateMetaData metaData = templatesResponse.getIndexTemplates().get(0);
        String mappingSource = metaData.mappings().get(Constants.DEFAULT_MAPPING_TYPE).toString();
        Map mapping = (Map) XContentFactory.xContent(mappingSource)
                .createParser(mappingSource)
                .mapAndClose()
                .get(Constants.DEFAULT_MAPPING_TYPE);
        assertNotNull(((Map) mapping.get("properties")).get("name"));
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
                " date timestamp" +
                ") clustered by(id) into 2 shards partitioned by(name) with(number_of_replicas=0)");

        execute("insert into locations_parted (id, name, date) (select id, name, date from locations)");
        assertThat(response.rowCount(), is(13L));

        execute("refresh table locations_parted");
        execute("select name from locations_parted order by id");
        assertThat(response.rowCount(), is(13L));
        assertThat((String) response.rows()[0][0], is(firstName));
    }

    @Test
    public void testPartitionedTableNestedPk() throws Exception {
        execute("create table t (o object as (i int primary key, name string)) partitioned by (o['i']) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (o) values (?)", new Object[]{new MapBuilder<String, Object>().put("i", 1).put("name", "Zaphod").map()});
        ensureGreen();
        refresh();
        execute("select o['i'], o['name'] from t");
        assertThat((Integer) response.rows()[0][0], Matchers.is(1));
        execute("select distinct table_name, partition_ident from sys.shards where table_name = 't'");
        assertEquals("t| 04132\n", TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void testStartPartitionWithMissingTable() throws Exception {
        // ensureYellow must succeed
        String partition = ".partitioned.parted.04130";
        client().admin().indices().prepareCreate(partition).execute().actionGet();
        ensureYellow();
    }

    @Test
    public void testCreateTableWithIllegalCustomSchemaCheckedByES() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("table name \"AAA.t\" is invalid.");
        execute("create table \"AAA\".t (name string, d timestamp) partitioned by (d) with (number_of_replicas=0)");
    }
}
