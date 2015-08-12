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
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLResponse;
import io.crate.executor.TaskResult;
import io.crate.metadata.PartitionName;
import io.crate.planner.Plan;
import io.crate.testing.TestingHelpers;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 2)
public class PartitionedTableIntegrationTest extends SQLTransportIntegrationTest {

    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(10, TimeUnit.SECONDS);

    private Setup setup = new Setup(sqlExecutor);

    private String copyFilePath = getClass().getResource("/essetup/data/copy").getPath();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCopyFromIntoPartitionedTableWithPARTITIONKeyword() throws Exception {
        execute("create table quotes (" +
                "id integer primary key," +
                "date timestamp primary key," +
                "quote string index using fulltext" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureYellow();
        String uriPath = Joiner.on("/").join(copyFilePath, "test_copy_from.json");
        execute("copy quotes partition (date=1400507539938) from ?", new Object[]{uriPath});
        assertEquals(3L, response.rowCount());
        refresh();
        execute("select id, date, quote from quotes order by id asc");
        assertEquals(3L, response.rowCount());
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Long) response.rows()[0][1], is(1400507539938L));
        assertThat((String) response.rows()[0][2], is("Don't pa\u00f1ic."));

        execute("select count(*) from information_schema.table_partitions where table_name = 'quotes'");
        assertThat((Long) response.rows()[0][0], is(1L));

        execute("copy quotes partition (date=1800507539938) from ?", new Object[]{uriPath});
        refresh();

        execute("select partition_ident from information_schema.table_partitions " +
                "where table_name = 'quotes' " +
                "order by partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("04732d1g60qj0dpl6csjicpo"));
        assertThat((String) response.rows()[1][0], is("04732e1g60qj0dpl6csjicpo"));
    }

    private TaskResult deletePartitionsAndExecutePlan(String stmt) throws Exception {
        execute("create table t (name string, p string) partitioned by (p)");
        execute("insert into t (name, p) values ('Arthur', 'a'), ('Trillian', 't')");
        ensureYellow();

        Plan plan = plan(stmt);
        execute("delete from t");
        ListenableFuture<List<TaskResult>> future = execute(plan);
        return future.get(500, TimeUnit.MILLISECONDS).get(0);
    }

    @Test
    public void testTableUnknownExceptionIsNotRaisedIfPartitionsAreDeletedAfterPlan() throws Exception {
        TaskResult taskResult = deletePartitionsAndExecutePlan("select * from t");
        assertThat(taskResult.rows().size(), is(0));
    }

    @Test
    public void testTableUnknownExceptionNotRaisedIfPartitionsDeletedAfterCountPlan() throws Exception {
        TaskResult taskResult = deletePartitionsAndExecutePlan("select count(*) from t");
        assertThat((Long)taskResult.rows().iterator().next().get(0), is(0L));
    }

    @Test
    public void testRefreshDuringPartitionDeletion() throws Exception {
        execute("create table t (name string, p string) partitioned by (p)");
        execute("insert into t (name, p) values ('Arthur', 'a'), ('Trillian', 't')");
        execute("refresh table t");

        Plan plan = plan("refresh table t"); // create a plan in which the partitions exist
        execute("delete from t");

        ListenableFuture<List<TaskResult>> future = execute(plan); // execute now that the partitions are gone
        // shouldn't throw an exception:
        future.get();
    }

    @Test
    public void testCopyFromIntoPartitionedTable() throws Exception {
        execute("create table quotes (" +
                "  id integer primary key, " +
                "  quote string index using fulltext" +
                ") partitioned by (id)");
        ensureYellow();

        String uriPath = Joiner.on("/").join(copyFilePath, "test_copy_from.json");
        execute("copy quotes from ?", new Object[]{uriPath});
        assertEquals(3L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();
        ensureYellow();

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
    public void testCopyFromPartitionedTableCustomSchema() throws Exception {
        execute("create table my_schema.parted (" +
                "  id long, " +
                "  month timestamp, " +
                "  created timestamp" +
                ") partitioned by (month) with (number_of_replicas=0)");
        ensureGreen();
        File copyFromFile = folder.newFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(copyFromFile));
        writer.write(
                "{\"id\":1, \"month\":1425168000000, \"created\":1425901500000}\n" +
                        "{\"id\":2, \"month\":1420070400000,\"created\":1425901460000}");
        writer.flush();
        writer.close();
        String uriPath = Paths.get(copyFromFile.toURI()).toString();

        execute("copy my_schema.parted from ? with (shared=true)", new Object[]{uriPath});
        assertEquals(2L, response.rowCount());
        refresh();

        ensureGreen();
        waitNoPendingTasksOnAll();

        execute("select schema_name, table_name, number_of_shards, number_of_replicas, clustered_by, partitioned_by " +
                "from information_schema.tables where schema_name='my_schema' and table_name='parted'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("my_schema| parted| 5| 0| _id| [month]\n"));

        // no other tables with that name, e.g. partitions considered as tables or such
        execute("select schema_name, table_name from information_schema.tables where table_name like '%parted%'");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "my_schema| parted\n"));

        execute("select count(*) from my_schema.parted");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long)response.rows()[0][0], is(2L));
    }

    @Test
    public void testCreatePartitionedTableAndQueryMeta() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();

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
        ensureYellow();
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
        ensureYellow();
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
        assertTrue(internalCluster().clusterService().state().metaData().hasIndex(partitionName));
        assertNotNull(client().admin().cluster().prepareState().execute().actionGet()
                .getState().metaData().indices().get(partitionName).aliases().get("parted"));
        assertThat(
                client().prepareCount(partitionName).setTypes(Constants.DEFAULT_MAPPING_TYPE)
                        .setQuery(new MatchAllQueryBuilder()).execute().actionGet().getCount(),
                is(1L)
        );

        partitionName = new PartitionName("parted", Arrays.asList(new BytesRef(String.valueOf(0L)))).stringValue();
        assertTrue(internalCluster().clusterService().state().metaData().hasIndex(partitionName));
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
        assertTrue(internalCluster().clusterService().state().metaData().hasIndex(partitionName));
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
    public void testBulkInsertPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
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
    public void testInsertPartitionedTableOnlyPartitionedColumns() throws Exception {
        execute("create table parted (name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureYellow();

        execute("insert into parted (name, date) values (?, ?)",
                new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
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
    public void testInsertPartitionedTableOnlyPartitionedColumnsAlreadyExists() throws Exception {
        execute("create table parted (name string, date timestamp)" +
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
    public void testInsertPartitionedTableSomePartitionedColumns() throws Exception {
        // insert only some partitioned column values
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (name, date)");
        ensureYellow();

        execute("insert into parted (id, name) values (?, ?)",
                new Object[]{1, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
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
        ensureYellow();

        Long dateValue = System.currentTimeMillis();
        execute("insert into parted (id, date, name) values (?, ?, ?)",
                new Object[]{1, dateValue, "Trillian"});
        assertThat(response.rowCount(), is(1L));
        ensureYellow();
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
    public void testSelectCountFromPartitionedTableWhereClause() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
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
    public void testSelectFromPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
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
        ensureYellow();
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{1, 127, "Don't panic"});
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{2, 126, "Time is an illusion. Lunchtime doubly so"});
        execute("insert into stuff (id, type, content) values(?, ?, ?)",
                new Object[]{3, 126, "Now panic"});
        ensureYellow();
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
    public void testUpdatePartitionedUnknownPartition() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp, o object) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
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
    public void testUpdatePartitionedUnknownColumn() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp, o object(ignored)) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
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
    public void testUpdateUnknownColumnKnownValueAndConjunction() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
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
    public void testDeleteFromPartitionedTable() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
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
        assertThat((String) response.rows()[0][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1388534400000"))).ident()));
        assertThat((String) response.rows()[1][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1391212800000"))).ident()));

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
                "where table_name='quotes' and schema_name='doc'" +
                "order by partition_ident");
        assertThat(response.rowCount(), is(3L));
        assertThat((String)response.rows()[0][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1395874800000"))).ident()));
        assertThat((String) response.rows()[1][0], is(new PartitionName("parted", ImmutableList.of(new BytesRef("1395961200000"))).ident()));
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
    public void testDeleteFromPartitionedTableDeleteByPartitionAndQueryWithConjunction() throws Exception {
        execute("create table quotes (id integer, quote string, timestamp timestamp) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{2, "Don't panic", 1395961200000L});
        execute("insert into quotes (id, quote, timestamp) values(?, ?, ?)",
                new Object[]{3, "Don't panic", 1396303200000L});
        ensureYellow();
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
    public void testGroupByPartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
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
    public void testGroupByPartitionedColumnWhereClause() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
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
        assertThat((Long) response.rows()[0][0], is(2437646253L));
        assertThat((Long) response.rows()[0][1], is(1L));
    }

    @Test
    public void testGlobalAggregateWhereClause() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date)");
        ensureYellow();
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
                "  date timestamp" +
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
                .prepareGetTemplates(PartitionName.templateName(null, "quotes")).execute().get();
        assertThat(getIndexTemplatesResponse.getIndexTemplates().size(), is(0));

        assertThat(internalCluster().clusterService().state().metaData().indices().size(), is(0));

        AliasesExistResponse aliasesExistResponse = client().admin().indices()
                .prepareAliasesExist("quotes").execute().get();
        assertFalse(aliasesExistResponse.exists());
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
    public void testPartitionedTableAllConstraintsRoundTrip() throws Exception {
        execute("create table quotes (id integer primary key, quote string, " +
                "date timestamp primary key, user_id string primary key) " +
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
    public void testPartitionedTableSchemaAllConstraintsRoundTrip() throws Exception {
        execute("create table my_schema.quotes (id integer primary key, quote string, " +
                "date timestamp primary key, user_id string primary key) " +
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
        execute("create table foo (" +
                "   id int primary key," +
                "   date timestamp primary key" +
                ") partitioned by (date) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into foo (id, date, foo) values (1, '2014-01-01', 'foo')");
        execute("insert into foo (id, date, bar) values (2, '2014-02-01', 'bar')");
        refresh();
        ensureYellow();

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
        ensureYellow();
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
                new Object[]{1, "Don't panic", new MapBuilder<String, Object>().put("date", 1395874800000L).put("user_id", "Arthur").map()});
        assertEquals(1L, response.rowCount());
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
                new Object[]{2, "Time is an illusion. Lunchtime doubly so", new MapBuilder<String, Object>().put("date", 1395961200000L).put("user_id", "Ford").map()});
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
        ensureYellow();
        assertThat(response.rowCount(), is(1L));

        String templateName = PartitionName.templateName(null, "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));

        execute("alter table quotes set (number_of_replicas=0)");
        ensureYellow();

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
        ensureYellow();
        refresh();

        assertTrue(clusterService().state().metaData().aliases().containsKey("quotes"));

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
        ensureYellow();
        assertThat(response.rowCount(), is(1L));

        String templateName = PartitionName.templateName(null, "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("1-all"));

        execute("alter table quotes reset (number_of_replicas)");
        ensureYellow();

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
        ensureYellow();
        assertThat(response.rowCount(), is(1L));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureYellow();
        refresh();

        execute("alter table quotes reset (number_of_replicas)");
        ensureYellow();

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
        ensureYellow();
        assertThat(response.rowCount(), is(1L));

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L,
                        2, "Now panic", 1395961200000L}
        );
        assertThat(response.rowCount(), is(2L));
        ensureYellow();
        refresh();

        execute("alter table quotes partition (date=1395874800000) set (number_of_replicas=1)");
        ensureYellow();
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
    public void testRefreshPartitionedTableAllPartitions() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) with (refresh_interval=0)");
        ensureYellow();

        execute("refresh table parted");
        assertThat(response.rowCount(), is(-1L));

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
        assertThat(response.rowCount(), is(-1L));

        // assert that all is available after refresh
        execute("select count(*) from parted");
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testRefreshEmptyPartitionedTable() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) with (refresh_interval=0)");
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("No partition for table 'parted' with ident '04130' exists");
        execute("refresh table parted partition(date=0)");
    }

    @Test
    public void testRefreshPartitionedTableSinglePartitions() throws Exception {
        execute("create table parted (id integer, name string, date timestamp) partitioned by (date) " +
                "with (number_of_replicas=0, refresh_interval=-1)");
        ensureYellow();
        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01')," +
                "(2, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount(), is(2L));

        ensureYellow();
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
    public void testRefreshMultipleTablesWithPartition() throws Exception {
        execute("create table t1 (" +
                "  id integer, " +
                "  name string, " +
                "  age integer, " +
                "  date timestamp) partitioned by (date, age) " +
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
        assertThat(response.rowCount(), is(-1L));

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
                .get(new PartitionName("dynamic_table", Arrays.asList(new BytesRef("10.0"))).stringValue())
                .getMappings().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(String.valueOf(partitionMetaData.getSourceAsMap().get("_meta")), Matchers.is("{partitioned_by=[[score, double]]}"));
        execute("alter table dynamic_table set (column_policy= 'dynamic')");
        waitNoPendingTasksOnAll();
        partitionMetaData = clusterService().state().metaData().indices()
                .get(new PartitionName("dynamic_table", Arrays.asList(new BytesRef("10.0"))).stringValue())
                .getMappings().get(Constants.DEFAULT_MAPPING_TYPE);
        assertThat(String.valueOf(partitionMetaData.getSourceAsMap().get("_meta")), Matchers.is("{partitioned_by=[[score, double]]}"));
    }

    @Test
    public void testCountPartitionedTable() throws Exception {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp" +
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
        assertThat((Long) response.rows()[0][0], is(2L));
    }

    @Test
    public void testAlterTableAddColumnOnPartitionedTableWithoutPartitions() throws Exception {
        execute("create table t (id int primary key, date timestamp primary key) " +
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
        ensureYellow();
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
        ensureYellow();
        execute("insert into t (o) values (?)", new Object[]{new MapBuilder<String, Object>().put("i", 1).put("name", "Zaphod").map()});
        ensureYellow();
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

    @Test
    public void testAlterNumberOfShards() throws Exception {
        execute("create table quotes (" +
                "  id integer, " +
                "  quote string, " +
                "  date timestamp) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas='0-all')");
        ensureYellow();
        assertThat(response.rowCount(), is(1L));

        String templateName = PartitionName.templateName(null, "quotes");
        GetIndexTemplatesResponse templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(3));

        execute("alter table quotes set (number_of_shards=6)");

        templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(6));

        execute("insert into quotes (id, quote, date) values (?, ?, ?)",
                new Object[]{1, "Don't panic", 1395874800000L}
        );
        assertThat(response.rowCount(), is(1L));
        refresh();

        assertTrue(clusterService().state().metaData().aliases().containsKey("quotes"));

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

        execute("select number_of_shards from information_schema.table_partitions where table_name='quotes' order by number_of_shards ASC");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer)response.rows()[0][0], is(2));
        assertThat((Integer)response.rows()[1][0], is(6));

        templatesResponse = client().admin().indices()
                .prepareGetTemplates(templateName).execute().actionGet();
        templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0), is(1));
        assertThat(templateSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0), is(2));
        assertThat(templateSettings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));

        List<String> partitions = ImmutableList.of(
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395874800000"))).stringValue(),
                new PartitionName("quotes", Arrays.asList(new BytesRef("1395961200000"))).stringValue()
        );
        Thread.sleep(1000);
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(
                partitions.get(0), partitions.get(1)
        ).execute().get();

        String index = partitions.get(0);
        assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_NUMBER_OF_SHARDS), is("6"));
        assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("1"));
        assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));

        index = partitions.get(1);
        assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_NUMBER_OF_SHARDS), is("2"));
        assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_NUMBER_OF_REPLICAS), is("1"));
        assertThat(settingsResponse.getSetting(index, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS), is("0-all"));

    }

    @Test
    public void testGroupOnDynamicObjectColumn() throws Exception {
        execute("create table event (day timestamp primary key, data object) clustered into 6 shards partitioned by (day)");
        ensureYellow();
        execute("insert into event (day, data) values ('2015-01-03', {sessionid = null})");
        execute("insert into event (day, data) values ('2015-01-01', {sessionid = 'hello'})");
        execute("refresh table event");
        waitForMappingUpdateOnAll("event", "data.sessionid");
        execute("select data['sessionid'] from event group by data['sessionid'] " +
                "order by format('%s', data['sessionid'])");
        assertThat(response.rows().length, Is.is(2));
        assertThat((String)response.rows()[0][0], Is.is("hello"));
        assertThat(response.rows()[1][0], Is.is(nullValue()));
    }

    @Test
    public void testFilterOnDynamicObjectColumn() throws Exception {
        execute("create table event (day timestamp primary key, data object) clustered into 6 shards partitioned by (day)");
        ensureYellow();
        execute("insert into event (day, data) values ('2015-01-03', {sessionid = null})");
        execute("insert into event (day, data) values ('2015-01-01', {sessionid = 'hello'})");
        execute("insert into event (day, data) values ('2015-02-08', {sessionid = 'ciao'})");
        execute("refresh table event");
        waitForMappingUpdateOnAll("event", "data.sessionid");
        execute("select data['sessionid'] from event where " +
                "format('%s', data['sessionid']) = 'ciao' order by data['sessionid']");
        assertThat(response.rows().length, Is.is(1));
        assertThat((String)response.rows()[0][0], Is.is("ciao"));
    }

    @Test
    public void testOrderByDynamicObjectColumn() throws Exception {
        execute("create table event (day timestamp primary key, data object, number int) clustered into 6 shards partitioned by (day)");
        ensureYellow();
        execute("insert into event (day, data, number) values ('2015-01-03', {sessionid = null}, 42)");
        execute("insert into event (day, data, number) values ('2015-01-01', {sessionid = 'hello'}, 42)");
        execute("insert into event (day, data, number) values ('2015-02-08', {sessionid = 'ciao'}, 42)");
        execute("refresh table event");
        waitForMappingUpdateOnAll("event", "data.sessionid");
        execute("select data['sessionid'] from event order by data['sessionid'] ASC nulls first");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "NULL\n" +
                "ciao\n" +
                "hello\n"));

        execute("select data['sessionid'] from event order by data['sessionid'] ASC nulls last");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "ciao\n" +
                "hello\n" +
                "NULL\n"));

        execute("select data['sessionid'] from event order by data['sessionid'] DESC nulls first");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "NULL\n" +
                "hello\n" +
                "ciao\n"));

        execute("select data['sessionid'] from event order by data['sessionid'] DESC nulls last");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "hello\n" +
                "ciao\n" +
                "NULL\n"));
    }

    @Test
    public void testDynamicColumnWhere() throws Exception {
        execute("create table event (day timestamp primary key, data object, number int) clustered into 6 shards partitioned by (day)");
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
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "{sessionid=null}\n" +
                "NULL\n"));

        execute("select data " +
                "from event " +
                "where data['sessionid'] = 'ciao'");
        assertThat(response.rowCount(), is(1L));

        execute("select data " +
                "from event " +
                "where data['sessionid'] in ('hello', 'goodbye') " +
                "order by number DESC");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "{sessionid=hello}\n"));
    }

    @Test
    public void testGroupOnDynamicColumn() throws Exception {
        execute("create table event (day timestamp primary key) clustered into 6 shards partitioned by (day)");
        ensureYellow();
        execute("insert into event (day) values ('2015-01-03')");
        execute("insert into event (day, sessionid) values ('2015-01-01', 'hello')");
        execute("refresh table event");

        waitForMappingUpdateOnAll("event", "sessionid");
        execute("select sessionid from event group by sessionid order by sessionid");
        assertThat(response.rows().length, Is.is(2));
        assertThat((String)response.rows()[0][0], Is.is("hello"));
        assertThat(response.rows()[1][0], Is.is(nullValue()));
    }

    @Test
    public void testFetchPartitionedTable() throws Exception {
        execute("SET GLOBAL stats.enabled = true");
        execute("create table fetch_partition_test (name string, p string) partitioned by (p) with (number_of_replicas=0)");
        ensureYellow();
        Object[][] bulkArgs = new Object[3][];
        for (int i = 0; i < 3; i++) {
            bulkArgs[i] = new Object[]{"Marvin", i};
        }
        execute("insert into fetch_partition_test (name, p) values (?, ?)", bulkArgs);
        execute("refresh table fetch_partition_test");
        execute("select count(*) from fetch_partition_test");
        assertThat(((long) response.rows()[0][0]), is(3L));
        execute("select count(*), job_id, arbitrary(name) from sys.operations_log where name='fetch' group by 2");
        assertThat(response.rowCount(), is(lessThanOrEqualTo(1L)));
    }


    @Test
    public void testDeleteOrphanedPartitions() throws Throwable {
        execute("create table foo (name string, p string) partitioned by (p) with (number_of_replicas=0, refresh_interval = 0)");
        ensureYellow();
        execute("insert into foo (name, p) values (?, ?)", new Object[]{"Marvin", 1});
        execute("refresh table foo");

        String templateName = PartitionName.templateName(null, "foo");
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
        // used to throw IndexMissingException if the new cluster state after the delete wasn't propagated to all nodes
        // (on about 2 runs in 100 iterations)
        execute("alter table t set (number_of_replicas = 0)");
    }

    @Test
    public void testSelectWhileShardsAreRelocating() throws Throwable {
        execute("create table t (name string, p string) " +
                "clustered into 2 shards " +
                "partitioned by (p) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (name, p) values (?, ?)", new Object[][] {
                new Object[] { "Marvin", "a" },
                new Object[] { "Trillian", "a" },
        });
        execute("refresh table t");
        execute("set global stats.enabled=true");

        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();
        final CountDownLatch selects = new CountDownLatch(100);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while (selects.getCount() > 0) {
                    try {
                        execute("select * from t");
                    } catch (Throwable t) {
                        // The failed job should have three started operations
                        SQLResponse res = execute("select id from sys.jobs_log where error is not null order by started desc limit 1");
                        if (res.rowCount() > 0) {
                            String id = (String) res.rows()[0][0];
                            res = execute("select count(*) from sys.operations_log where name=? or name = ?and job_id = ?", new Object[]{"collect", "fetchContext", id});
                            if ((long) res.rows()[0][0] < 3) {
                                // set the error if there where less than three attempts
                                lastThrowable.set(t);
                            }
                        }
                    } finally {
                        selects.countDown();
                    }
                }
            }
        });
        t.start();

        PartitionName partitionName = new PartitionName("t", Collections.singletonList(new BytesRef("a")));
        final String indexName = partitionName.stringValue();

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        DiscoveryNodes nodes = clusterService.state().nodes();
        List<String> nodeIds = new ArrayList<>(2);
        for (DiscoveryNode node : nodes) {
            if (node.dataNode()) {
                nodeIds.add(node.id());
            }
        }
        final Map<String, String> nodeSwap = new HashMap<>(2);
        nodeSwap.put(nodeIds.get(0), nodeIds.get(1));
        nodeSwap.put(nodeIds.get(1), nodeIds.get(0));

        final CountDownLatch relocations = new CountDownLatch(20);
        Thread relocatingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (relocations.getCount() > 0) {
                    ClusterStateResponse clusterStateResponse = admin().cluster().prepareState().setIndices(indexName).execute().actionGet();
                    List<ShardRouting> shardRoutings = clusterStateResponse.getState().routingTable().allShards(indexName);

                    ClusterRerouteRequestBuilder clusterRerouteRequestBuilder = admin().cluster().prepareReroute();
                    int numMoves = 0;
                    for (ShardRouting shardRouting : shardRoutings) {
                        if (shardRouting.currentNodeId() == null) {
                            continue;
                        }
                        if (shardRouting.state() != ShardRoutingState.STARTED) {
                            continue;
                        }
                        String toNode = nodeSwap.get(shardRouting.currentNodeId());
                        clusterRerouteRequestBuilder.add(new MoveAllocationCommand(
                                shardRouting.shardId(),
                                shardRouting.currentNodeId(),
                                toNode));
                        numMoves++;
                    }

                    if (numMoves > 0) {
                        clusterRerouteRequestBuilder.execute().actionGet();
                        client().admin().cluster().prepareHealth()
                                .setWaitForEvents(Priority.LANGUID)
                                .setWaitForRelocatingShards(0)
                                .setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();
                        relocations.countDown();
                    }
                }
            }
        });
        relocatingThread.start();
        relocations.await();
        selects.await();

        Throwable throwable = lastThrowable.get();
        if (throwable != null) {
            throw throwable;
        }
    }

    @After
    @Override
    public void tearDown() throws Exception {
        execute("RESET GLOBAL stats.enabled");
        super.tearDown();
    }
}
