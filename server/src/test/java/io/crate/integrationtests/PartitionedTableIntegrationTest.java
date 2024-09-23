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

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNIQUE_VIOLATION;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.BAD_REQUEST;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.INTERNAL_SERVER_ERROR;

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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.session.BaseResultReceiver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.testing.Asserts;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseNewCluster;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;

@UseRandomizedOptimizerRules(0)
@IntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 2)
public class PartitionedTableIntegrationTest extends IntegTestCase {

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
        assertThat(response.rowCount()).isEqualTo(3L);
        execute("refresh table quotes");
        execute("select id, date, quote from quotes order by id asc");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo(1400507539938L);
        assertThat(response.rows()[0][2]).isEqualTo("Don't pa\u00f1ic.");

        execute("select count(*) from information_schema.table_partitions where table_name = 'quotes'");
        assertThat((Long) response.rows()[0][0]).isEqualTo(1L);

        execute("copy quotes partition (date=1800507539938) from ?", new Object[]{
            copyFilePath + "test_copy_from.json"});
        execute("refresh table quotes");

        execute("select partition_ident from information_schema.table_partitions " +
                "where table_name = 'quotes' " +
                "order by partition_ident");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("04732d1g60qj0dpl6csjicpo");
        assertThat(response.rows()[1][0]).isEqualTo("04732e1g60qj0dpl6csjicpo");
    }

    @Test
    public void testCopyFromIntoPartitionedTable() throws Exception {
        execute("create table quotes (" +
                "  id integer primary key, " +
                "  quote string index using fulltext" +
                ") partitioned by (id)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response.rowCount()).isEqualTo(3L);
        execute("refresh table quotes");
        ensureYellow();

        ClusterState state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        for (String id : List.of("1", "2", "3")) {
            String partitionName = new PartitionName(
                new RelationName(sqlExecutor.getCurrentSchema(), "quotes"),
                List.of(id)).asIndexName();
            var partitionIndexMetadata = state.metadata().indices().get(partitionName);
            assertThat(partitionIndexMetadata).isNotNull();
            assertThat(partitionIndexMetadata.getAliases().get(getFqn("quotes"))).isNotNull();
        }

        execute("select * from quotes");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0].length).isEqualTo(2);
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
        assertThat(response.rowCount()).isEqualTo(3L);
        execute("refresh table quotes");
        ensureYellow();

        execute("select * from quotes");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0].length).isEqualTo(3);
    }

    @Test
    public void testInsertIntoClosedPartition() throws Exception {
        int numberOfReplicas = numberOfReplicas();
        execute("create table t (a integer, b string) partitioned by (a) with (number_of_replicas = ?)", $(numberOfReplicas));
        execute("insert into t (a, b) values (1, 'foo')");
        ensureGreen();

        execute("alter table t partition (a = 1) close");
        assertBusy(() -> {
            assertThat(execute("select closed from information_schema.table_partitions where table_name = 't'"))
                .hasRows($(true));
        });

        execute("insert into t (a, b) values (1, 'bar')");
        assertThat(response.rowCount()).isEqualTo(0L);
        execute("select count(*) from t");
        assertThat(response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void testSelectFromClosedPartition() throws Exception {
        execute("create table t (n integer) partitioned by (n) with (number_of_replicas = ?)", $(numberOfReplicas()));
        execute("insert into t (n) values (1)");
        ensureGreen();

        execute("alter table t partition (n = 1) close");
        assertBusy(() -> {
            assertThat(execute("select closed from information_schema.table_partitions where table_name = 't'"))
                .hasRows($(true));
        });
        execute("select count(*) from t");
        assertThat(response.rows()[0][0]).isEqualTo(0L);
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
        assertThat(response.rowCount()).isEqualTo(2L);
        execute("refresh table my_schema.parted");

        ensureGreen();
        waitNoPendingTasksOnAll();

        execute("select table_schema, table_name, number_of_shards, number_of_replicas, clustered_by, partitioned_by " +
                "from information_schema.tables where table_schema='my_schema' and table_name='parted'");
        assertThat(printedTable(response.rows())).isEqualTo("my_schema| parted| 4| 0| _id| [month]\n");

        // no other tables with that name, e.g. partitions considered as tables or such
        execute("select table_schema, table_name from information_schema.tables where table_name like '%parted%'");
        assertThat(printedTable(response.rows())).isEqualTo(
            "my_schema| parted\n");

        execute("select count(*) from my_schema.parted");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(2L);
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
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][12]).isEqualTo("quotes");
        assertThat(response.rows()[0][8]).isEqualTo(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME);
        assertThat((boolean) response.rows()[0][1]).isFalse();
        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        execute("select * from information_schema.columns where table_name='quotes' order by ordinal_position");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0][12]).isEqualTo("id");
        assertThat(response.rows()[1][12]).isEqualTo("quote");
        assertThat(response.rows()[2][12]).isEqualTo("timestamp");
    }

    @Test
    public void testInsertPartitionedTable() throws Exception {
        execute(
            "create table parted (" +
            "   id integer," +
            "   name string," +
            "   date timestamp with time zone" +
            ") partitioned by (date)");

        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "parted");

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{1, "Ford", 13959981214861L});
        assertThat(response).hasRowCount(1);
        ensureYellow();
        execute("refresh table parted");

        Metadata metadata = clusterService().state().metadata();
        String fqTablename = getFqn("parted");
        assertThat(metadata.hasAlias(fqTablename)).isTrue();
        assertThat(metadata.templates().containsKey(templateName)).isTrue();

        String partitionName = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "parted"),
            Collections.singletonList(String.valueOf(13959981214861L))
        ).asIndexName();

        assertThat(metadata.indices().get(partitionName).getAliases().get(fqTablename)).isNotNull();

        execute("select id, name, date from parted");
        assertThat(response).hasRows(
            "1| Ford| 13959981214861"
        );
    }

    private void validateInsertPartitionedTable() {
        execute("select table_name, partition_ident from information_schema.table_partitions order by 2 ");
        assertThat(
            printedTable(response.rows())).isEqualTo("parted| 0400\n" +
               "parted| 04130\n" +
               "parted| 047j2cpp6ksjie1h68oj8e1m64\n");
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
        assertThat(response.rowCount()).isEqualTo(3L);
        ensureYellow();
        execute("refresh table parted");

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
        execute("refresh table parted");

        validateInsertPartitionedTable();
    }

    @Test
    public void testInsertPartitionedTableOnlyPartitionedColumns() {
        execute("create table parted (name string, date timestamp with time zone)" +
                "partitioned by (name, date)");
        ensureYellow();

        execute("insert into parted (name, date) values (?, ?)",
            new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("refresh table parted");

        PartitionName partitionName = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "parted"),
            Arrays.asList("Ford", String.valueOf(13959981214861L))
        );

        execute(
            "select count(*) from information_schema.table_partitions where partition_ident = ?",
            $(partitionName.ident()));
        assertThat(response.rows()[0][0]).isEqualTo(1L);

        execute("select date, name from parted");
        assertThat(
            printedTable(response.rows())).isEqualTo("13959981214861| Ford\n");
    }

    @Test
    public void testInsertPartitionedTableOnlyPartitionedColumnsAlreadyExists() {
        execute("create table parted (name string, date timestamp with time zone)" +
                "partitioned by (name, date)");
        ensureYellow();

        execute("insert into parted (name, date) values (?, ?)",
            new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount()).isEqualTo(1L);
        ensureYellow();
        execute("refresh table parted");
        execute("insert into parted (name, date) values (?, ?)",
            new Object[]{"Ford", 13959981214861L});
        assertThat(response.rowCount()).isEqualTo(1L);
        ensureYellow();
        execute("refresh table parted");

        execute("select name, date from parted");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("Ford");
        assertThat(response.rows()[1][0]).isEqualTo("Ford");

        assertThat(response.rows()[0][1]).isEqualTo(13959981214861L);
        assertThat(response.rows()[1][1]).isEqualTo(13959981214861L);
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
        assertThat(response.rowCount()).isEqualTo(1L);
        ensureYellow();
        execute("refresh table parted");

        Asserts.assertSQLError(() -> execute("insert into parted (id, name, date) values (?, ?, ?)",
                                   new Object[]{42, "Zaphod", 0L}))
            .hasPGError(UNIQUE_VIOLATION)
            .hasHTTPError(CONFLICT, 4091)
            .hasMessageContaining("A document with the same primary key exists already");
    }

    @Test
    public void testInsertPartitionedTableReversedPartitionedColumns() throws Exception {
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
        assertThat(response.rowCount()).isEqualTo(1L);
        ensureYellow();
        execute("refresh table parted");
        String partitionName = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "parted"),
            Arrays.asList("Trillian", dateValue.toString())).asIndexName();
        assertThat(client().admin().cluster().state(new ClusterStateRequest()).get()
            .getState().metadata().indices().get(partitionName).getAliases().get(getFqn("parted")))
            .isNotNull();
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
        execute("refresh table parted_generated");

        execute("select day from parted_generated");
        assertThat(response.rows()[0][0]).isEqualTo(1448236800000L);
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
        execute("refresh table quotes");

        execute("select id, quote from quotes where (timestamp = 1395961200000 or timestamp = 1395874800000) and id = 1");
        assertThat(response.rowCount()).isEqualTo(1L);
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
        execute("refresh table quotes");

        execute("select count(*) from quotes where (timestamp = 1395961200000 or timestamp = 1395874800000)");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(2L);

        execute("select count(*) from quotes where timestamp = 1");
        assertThat(response.rows()[0][0]).isEqualTo(0L);
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
        execute("refresh table quotes");
        execute("select id, quote, timestamp as ts, timestamp from quotes where timestamp > 1395874800000");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(2);
        assertThat(response.rows()[0][1]).isEqualTo("Time is an illusion. Lunchtime doubly so");
        assertThat(response.rows()[0][2]).isEqualTo(1395961200000L);
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
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Integer) response.rows()[0][0]).isEqualTo(2);
        byte b = 126;
        assertThat((Byte) response.rows()[0][1]).isEqualTo(b);
        assertThat((String) response.rows()[0][2]).isEqualTo("Time is an illusion. Lunchtime doubly so");

        // multiget
        execute("select id, type, content from stuff where id in (2, 3) and type=126 order by id");
        assertThat(response.rowCount()).isEqualTo(2L);

        assertThat((Integer) response.rows()[0][0]).isEqualTo(2);
        assertThat((Byte) response.rows()[0][1]).isEqualTo(b);
        assertThat((String) response.rows()[0][2]).isEqualTo("Time is an illusion. Lunchtime doubly so");

        assertThat((Integer) response.rows()[1][0]).isEqualTo(3);
        assertThat((Byte) response.rows()[1][1]).isEqualTo(b);
        assertThat((String) response.rows()[1][2]).isEqualTo("Now panic");
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
        execute("refresh table quotes");

        execute("update quotes set quote = ? where timestamp = ?",
            new Object[]{"I'd far rather be happy than right any day.", 1395874800000L});
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table quotes");

        execute("select id, quote from quotes where timestamp = 1395874800000");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo("I'd far rather be happy than right any day.");

        execute("update quotes set quote = ?",
            new Object[]{"Don't panic"});
        assertThat(response.rowCount()).isEqualTo(2L);
        execute("refresh table quotes");

        execute("select id, quote from quotes where quote = ?",
            new Object[]{"Don't panic"});
        assertThat(response.rowCount()).isEqualTo(2L);
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
        execute("refresh table quotes");

        execute("update quotes set quote='now panic' where timestamp = ?", new Object[]{1395874800123L});
        execute("refresh table quotes");

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount()).isEqualTo(0L);
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
        execute("refresh table quotes");

        execute("update quotes set quote='now panic' where o['timestamp'] = ?", new Object[]{1395874800123L});
        execute("refresh table quotes");

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount()).isEqualTo(0L);
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
        execute("refresh table quotes");

        execute("update quotes set quote='now panic' where timestamp = ? and quote=?",
            new Object[]{1395874800123L, "Don't panic"});
        assertThat(response.rowCount()).isEqualTo(0L);
        execute("refresh table quotes");

        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount()).isEqualTo(0L);
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
        execute("refresh table quotes");

        execute("update quotes set quote='now panic' where not timestamp = ? and quote=?",
            new Object[]{1395874800000L, "Don't panic"});
        execute("refresh table quotes");
        execute("select * from quotes where quote = 'now panic'");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testUpdateByQueryOnEmptyPartitionedTable() {
        execute("create table empty_parted(id integer, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();

        execute("update empty_parted set id = 10 where timestamp = 1396303200000");
        assertThat(response.rowCount()).isEqualTo(0L);
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
        execute("refresh table quotes");

        execute("delete from quotes where timestamp = 1395874800000 and id = 1");
        assertThat(response.rowCount()).isEqualTo(1);
        execute("refresh table quotes");

        execute("select id, quote from quotes where timestamp = 1395874800000");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("select id, quote from quotes");
        assertThat(response.rowCount()).isEqualTo(2L);

        execute("delete from quotes");
        assertThat(response.rowCount()).isIn(0L, -1L);
        execute("refresh table quotes");

        execute("select id, quote from quotes");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testDeleteFromPartitionedTableUnknownPartition() throws Exception {
        this.setup.partitionTableSetup();
        String defaultSchema = sqlExecutor.getCurrentSchema();
        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                                       "where table_name='parted' and table_schema = ? " +
                                       "order by partition_ident", new Object[]{defaultSchema});
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat((String) response.rows()[0][0]).isEqualTo(new PartitionName(
            new RelationName(defaultSchema, "parted"), List.of("1388534400000")).ident());
        assertThat((String) response.rows()[1][0]).isEqualTo(new PartitionName(
            new RelationName(defaultSchema, "parted"), List.of("1391212800000")).ident());

        execute("delete from parted where date = '2014-03-01'");
        execute("refresh table parted");
        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                                          "where table_name='parted' and table_schema = ? " +
                                          "order by partition_ident", new Object[]{defaultSchema});
        assertThat(newResponse.rows()).isEqualTo(response.rows());
    }

    @Test
    public void testDeleteFromPartitionedTableWrongPartitionedColumn() throws Exception {
        this.setup.partitionTableSetup();
        String defaultSchema = sqlExecutor.getCurrentSchema();
        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                                       "where table_name='parted' and table_schema = ? " +
                                       "order by partition_ident", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat((String) response.rows()[0][0]).isEqualTo(new PartitionName(
            new RelationName(defaultSchema, "parted"), List.of("1388534400000")).ident());
        assertThat((String) response.rows()[1][0]).isEqualTo(new PartitionName(
            new RelationName(defaultSchema, "parted"), List.of("1391212800000")).ident());

        execute("delete from parted where o['dat'] = '2014-03-01'");
        execute("refresh table parted");
        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                                          "where table_name='parted' and table_schema = ? " +
                                          "order by partition_ident", new Object[]{defaultSchema});
        assertThat(newResponse.rows()).isEqualTo(response.rows());
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
        execute("refresh table quotes");

        SQLResponse response = execute("select partition_ident from information_schema.table_partitions " +
                                       "where table_name='quotes' and table_schema = ? " +
                                       "order by partition_ident", new Object[]{defaultSchema});
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat((String) response.rows()[0][0]).isEqualTo(new PartitionName(
            new RelationName(defaultSchema, "parted"), List.of("1395874800000")).ident());
        assertThat((String) response.rows()[1][0]).isEqualTo(new PartitionName(
            new RelationName(defaultSchema, "parted"), List.of("1395961200000")).ident());
        assertThat((String) response.rows()[2][0]).isEqualTo(new PartitionName(
            new RelationName(defaultSchema, "parted"), List.of("1396303200000")).ident());

        execute("delete from quotes where quote = 'Don''t panic'");
        execute("refresh table quotes");

        execute("select * from quotes where quote = 'Don''t panic'");
        assertThat(this.response.rowCount()).isEqualTo(0L);

        // Test that no partitions were deleted
        SQLResponse newResponse = execute("select partition_ident from information_schema.table_partitions " +
                                          "where table_name='quotes' and table_schema = ? " +
                                          "order by partition_ident", new Object[]{defaultSchema});
        assertThat(newResponse.rows()).isEqualTo(response.rows());
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
        execute("refresh table quotes");

        // does not match
        execute("delete from quotes where quote = 'Don''t panic' and timestamp=?", new Object[]{1396303200000L});
        execute("refresh table quotes");
        execute("select * from quotes where timestamp=?", new Object[]{1396303200000L});
        assertThat(response.rowCount()).isEqualTo(1L);

        // matches
        execute("delete from quotes where quote = 'I''d far rather be happy than right any day' and timestamp=?", new Object[]{1396303200000L});
        execute("refresh table quotes");
        execute("select * from quotes where timestamp=?", new Object[]{1396303200000L});
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("delete from quotes where timestamp=? and o['x']=5", new Object[]{1395874800000L});
        execute("refresh table quotes");
        execute("select * from quotes where timestamp=?", new Object[]{1395874800000L});
        assertThat(response.rowCount()).isEqualTo(2L);


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
        execute("refresh table quotes");

        execute("delete from quotes where not timestamp=? and quote=?", new Object[]{1396303200000L, "Don't panic"});
        execute("refresh table quotes");
        execute("select * from quotes");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testDeleteByQueryFromEmptyPartitionedTable() {
        execute("create table empty_parted (id integer, timestamp timestamp with time zone) " +
                "partitioned by(timestamp) with (number_of_replicas=0)");
        ensureYellow();

        execute("delete from empty_parted where not timestamp = 1396303200000");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testGlobalAggregatePartitionedColumns() throws Exception {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(0L);
        assertThat((Long) response.rows()[0][1]).isEqualTo(0L);
        assertThat(response.rows()[0][2]).isNull();
        assertThat(response.rows()[0][3]).isNull();
        assertThat(response.rows()[0][4]).isNull();
        assertThat(response.rows()[0][5]).isNull();

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{0, "Trillian", 100L});
        ensureYellow();
        execute("refresh table parted");

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(1L);
        assertThat((Long) response.rows()[0][1]).isEqualTo(1L);
        assertThat((Long) response.rows()[0][2]).isEqualTo(100L);
        assertThat((Long) response.rows()[0][3]).isEqualTo(100L);
        assertThat((Long) response.rows()[0][4]).isEqualTo(100L);
        assertThat((Double) response.rows()[0][5]).isEqualTo(100.0);

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{1, "Ford", 1001L});
        ensureYellow();
        execute("refresh table parted");

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{2, "Arthur", 1001L});
        ensureYellow();
        execute("refresh table parted");

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(2L);
        assertThat((Long) response.rows()[0][1]).isEqualTo(3L);
        assertThat((Long) response.rows()[0][2]).isEqualTo(100L);
        assertThat((Long) response.rows()[0][3]).isEqualTo(1001L);
        assertThat((Long) response.rows()[0][4]).isIn(100L, 1001L);
        assertThat((Double) response.rows()[0][5]).isEqualTo(700.6666666666666);
    }

    @Test
    public void testGroupByPartitionedColumns() {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("select date, count(*) from parted group by date");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{0, "Trillian", 100L});
        ensureYellow();
        execute("refresh table parted");

        execute("select date, count(*) from parted group by date");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(100L);
        assertThat((Long) response.rows()[0][1]).isEqualTo(1L);

        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Arthur", null,
                2, "Ford", null
            });
        ensureYellow();
        execute("refresh table parted");

        execute("select date, count(*) from parted group by date order by count(*) desc");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isNull();
        assertThat((Long) response.rows()[0][1]).isEqualTo(2L);
        assertThat((Long) response.rows()[1][0]).isEqualTo(100L);
        assertThat((Long) response.rows()[1][1]).isEqualTo(1L);
    }

    @Test
    public void testGroupByPartitionedColumnWhereClause() {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("select date, count(*) from parted where date > 0 group by date");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("insert into parted (id, name, date) values (?, ?, ?)",
            new Object[]{0, "Trillian", 100L});
        ensureYellow();
        execute("refresh table parted");

        execute("select date, count(*) from parted where date > 0 group by date");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Arthur", 0L,
                2, "Ford", 2437646253L
            }
        );
        ensureYellow();
        execute("refresh table parted");

        execute("select date, count(*) from parted where date > 100 group by date");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(2437646253L);
        assertThat(response.rows()[0][1]).isEqualTo(1L);
    }

    @Test
    public void testGlobalAggregateWhereClause() {
        execute("create table parted (id integer, name string, date timestamp with time zone)" +
                "partitioned by (date)");
        ensureYellow();
        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted where date > 0");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(0L);
        assertThat(response.rows()[0][1]).isEqualTo(0L);
        assertThat(response.rows()[0][2]).isNull();
        assertThat(response.rows()[0][3]).isNull();
        assertThat(response.rows()[0][4]).isNull();
        assertThat(response.rows()[0][5]).isNull();

        execute("insert into parted (id, name, date) values " +
                "(?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Arthur", 0L,
                2, "Ford", 2437646253L,
                3, "Zaphod", 1L,
                4, "Trillian", 0L
            });
        assertThat(response.rowCount()).isEqualTo(4L);
        ensureYellow();
        execute("refresh table parted");

        execute("select count(distinct date), count(*), min(date), max(date), " +
                "arbitrary(date) as any_date, avg(date) from parted where date > 0");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(2L);
        assertThat((Long) response.rows()[0][1]).isEqualTo(2L);
        assertThat((Long) response.rows()[0][2]).isEqualTo(1L);
        assertThat((Long) response.rows()[0][3]).isEqualTo(2437646253L);
        assertThat((Long) response.rows()[0][4]).isIn(1L, 2437646253L);
        assertThat((Double) response.rows()[0][5]).isEqualTo(1.218823127E9);
    }

    @Test
    public void testDropPartitionedTable() throws Exception {
        execute("create table quotes (" +
                "  id integer, " +
                "  quote string, " +
                "  date timestamp with time zone" +
                ") partitioned by (date) with (number_of_replicas=0)");
        execute("insert into quotes (id, quote, date) values(?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L,
                2, "Time is an illusion. Lunchtime doubly so", 1395961200000L});
        ensureYellow();
        execute("refresh table quotes");

        execute("drop table quotes");
        assertThat(response).hasRowCount(1);

        ClusterState state = cluster().clusterService().state();
        assertThat(state.metadata().indices()).isEmpty();
        assertThat(state.metadata().hasAlias("quotes")).isFalse();
        assertThat(state.metadata().templates()).isEmpty();
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
        execute("refresh table quotes");
        execute("select * from quotes where id = 1 and num = 4");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(String.join(", ", response.cols())).isEqualTo("id, quote, num");
        assertThat((Integer) response.rows()[0][0]).isEqualTo(1);
        assertThat((String) response.rows()[0][1]).isEqualTo("Don't panic");
        assertThat((Double) response.rows()[0][2]).isEqualTo(4.0d);
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
                    }}
            });
        ensureYellow();
        execute("refresh table quotes");

        execute("select * from information_schema.columns where table_name = 'quotes'");
        assertThat(response.rowCount()).isEqualTo(5L);

        execute("insert into quotes (id, quote, date, author) values(?, ?, ?, ?)",
            new Object[]{3, "I'd far rather be happy than right any day", 1395874800000L,
                new HashMap<String, Object>() {{
                        put("name", "Douglas");
                        put("surname", "Adams");
                    }}
            });
        ensureYellow();
        execute("refresh table quotes");

        execute("select * from information_schema.columns where table_name = 'quotes'");
        assertThat(response.rowCount()).isEqualTo(6L);

        execute("select author['surname'] from quotes order by id");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0][0]).isNull();
        assertThat(response.rows()[1][0]).isNull();
        assertThat(response.rows()[2][0]).isEqualTo("Adams");
    }

    @Test
    public void testPartitionedTableAllConstraintsRoundTrip() {
        execute("create table quotes (id integer primary key, quote string, " +
                "date timestamp with time zone primary key, user_id string primary key) " +
                "partitioned by(date, user_id) clustered by (id) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L, "Arthur"});
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("insert into quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L, "Ford"});
        assertThat(response.rowCount()).isEqualTo(1L);
        ensureYellow();
        execute("refresh table quotes");

        execute("select id, quote from quotes where user_id = 'Arthur'");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("update quotes set quote = ? where user_id = ?",
            new Object[]{"I'd far rather be happy than right any day", "Arthur"});
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table quotes");

        execute("delete from quotes where user_id = 'Arthur' and id = 1 and date = 1395874800000");
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table quotes");

        execute("select * from quotes");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("delete from quotes"); // this will delete all partitions
        execute("delete from quotes"); // this should still work even though only the template exists

        execute("drop table quotes");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testPartitionedTableSchemaAllConstraintsRoundTrip() {
        execute("create table my_schema.quotes (id integer primary key, quote string, " +
                "date timestamp with time zone primary key, user_id string primary key) " +
                "partitioned by(date, user_id) clustered by (id) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into my_schema.quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L, "Arthur"});
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("insert into my_schema.quotes (id, quote, date, user_id) values(?, ?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", 1395961200000L, "Ford"});
        assertThat(response.rowCount()).isEqualTo(1L);
        ensureYellow();
        execute("refresh table my_schema.quotes");

        execute("select id, quote from my_schema.quotes where user_id = 'Arthur'");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("update my_schema.quotes set quote = ? where user_id = ?",
            new Object[]{"I'd far rather be happy than right any day", "Arthur"});
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table my_schema.quotes");

        execute("delete from my_schema.quotes where user_id = 'Arthur' and id = 1 and date = 1395874800000");
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table my_schema.quotes");

        execute("select * from my_schema.quotes");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("delete from my_schema.quotes"); // this will delete all partitions
        execute("delete from my_schema.quotes"); // this should still work even though only the template exists

        execute("drop table my_schema.quotes");
        assertThat(response.rowCount()).isEqualTo(1L);
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
        execute("refresh table t1");
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
        assertThat(retry < 100).isTrue();
    }

    @Test
    public void testPartitionedTableNestedAllConstraintsRoundTrip() {
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
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("insert into quotes (id, quote, created) values(?, ?, ?)",
            new Object[]{2, "Time is an illusion. Lunchtime doubly so", Map.of("date", 1395961200000L, "user_id", "Ford")});
        assertThat(response.rowCount()).isEqualTo(1L);
        ensureYellow();
        execute("refresh table quotes");

        execute("select id, quote, created['date'] from quotes where created['user_id'] = 'Arthur'");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][2]).isEqualTo(1395874800000L);

        execute("update quotes set quote = ? where created['date'] = ?",
            new Object[]{"I'd far rather be happy than right any day", 1395874800000L});
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("refresh table quotes");

        execute("select count(*) from quotes where quote=?", new Object[]{"I'd far rather be happy than right any day"});
        assertThat((Long) response.rows()[0][0]).isEqualTo(1L);

        execute("delete from quotes where created['user_id'] = 'Arthur' and id = 1 and created['date'] = 1395874800000");
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table quotes");

        execute("select * from quotes");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("drop table quotes");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testAlterNumberOfReplicas() throws Exception {
        String defaultSchema = sqlExecutor.getCurrentSchema();
        execute(
            "create table tbl (id int, p int) partitioned by (p) " +
            "clustered into 3 shards with (number_of_replicas = '0-all')"
        );

        execute("select number_of_replicas from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("0-all");
        execute("alter table tbl set (number_of_replicas=0)");
        execute("select number_of_replicas from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("0");

        execute("select count(*) from sys.shards where id = 0 and table_name = 'tbl'");
        assertThat(response)
            .as("has no partitions yet")
            .hasRows("0");

        execute("insert into tbl (id, p) values (1, 1), (2, 2)");
        assertThat(response).hasRowCount(2);
        ensureYellow();
        execute("refresh table tbl");

        assertThat(clusterService().state().metadata().hasAlias(getFqn("tbl"))).isTrue();

        RelationName relationName = new RelationName(defaultSchema, "tbl");
        List<String> partitions = List.of(
            new PartitionName(relationName, List.of("1")).asIndexName(),
            new PartitionName(relationName, List.of("2")).asIndexName()
        );

        execute("select number_of_replicas from information_schema.table_partitions");
        assertThat(response).hasRows(
            "0",
            "0"
        );

        execute("alter table tbl set (number_of_replicas='1-all')");
        ensureYellow();

        execute("select number_of_replicas from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("1-all");


        execute("select number_of_replicas from information_schema.table_partitions");
        assertThat(response).hasRows(
            "1-all",
            "1-all"
        );

        execute("select count(*) from sys.shards where table_name = 'tbl'");
        assertThat(response)
            .as("3 shards per data node and partition due to `1-all`")
            .hasRows(Integer.toString(2 * 3 * cluster().numDataNodes()));
    }

    @Test
    public void test_can_reset_number_of_replicas_on_partitioned_table() throws Exception {
        execute("create table tbl (id int, p int) partitioned by (p) with (number_of_replicas = 1)");
        execute("insert into tbl (id, p) values (1, 1)");

        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "tbl");
        IndexTemplateMetadata indexTemplateMetadata = clusterService().state().metadata().templates().get(templateName);
        Settings settings = indexTemplateMetadata.settings();

        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings)).isEqualTo(1);
        AutoExpandReplicas autoExpand1 = AutoExpandReplicas.SETTING.get(settings);
        assertThat(autoExpand1.isEnabled()).isFalse();

        execute("alter table tbl reset (number_of_replicas)");

        indexTemplateMetadata = clusterService().state().metadata().templates().get(templateName);
        settings = indexTemplateMetadata.settings();

        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings)).isEqualTo(0);
        AutoExpandReplicas autoExpand2 = AutoExpandReplicas.SETTING.get(settings);
        assertThat(autoExpand2.isEnabled()).isTrue();
        assertThat(autoExpand2.toString()).isEqualTo("0-1");

        assertBusy(() -> {
            execute("select number_of_replicas from information_schema.table_partitions");
            assertThat(response).hasRows("0-1");
        });
    }

    @Test
    public void testAlterPartitionedTablePartition() throws Exception {
        execute("create table quotes (id integer, quote string, date timestamp with time zone) " +
                "partitioned by(date) clustered into 3 shards with (number_of_replicas=0)");

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );
        assertThat(response).hasRowCount(2);
        ensureYellow();
        execute("refresh table quotes");

        execute("alter table quotes partition (date=1395874800000) set (number_of_replicas=1)");
        ensureYellow();

        execute("select partition_ident, number_of_replicas from information_schema.table_partitions order by 1");
        assertThat(response).hasRows(
            "04732cpp6ks3ed1o60o30c1g| 1",
            "04732cpp6ksjcc9i60o30c1g| 0"
        );

        String templateName = PartitionName.templateName(sqlExecutor.getCurrentSchema(), "quotes");
        IndexTemplateMetadata indexTemplateMetadata = clusterService().state().metadata().templates().get(templateName);
        Settings settings = indexTemplateMetadata.settings();

        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings)).isEqualTo(0);
        AutoExpandReplicas autoExpand1 = AutoExpandReplicas.SETTING.get(settings);
        assertThat(autoExpand1.isEnabled()).isFalse();
    }

    @Test
    public void testAlterPartitionedTableSettings() throws Exception {
        execute("create table attrs (name string, attr string, value integer) " +
                "partitioned by (name) clustered into 1 shards with (number_of_replicas=0, \"routing.allocation.total_shards_per_node\"=5)");
        ensureYellow();
        execute("insert into attrs (name, attr, value) values (?, ?, ?), (?, ?, ?)",
            new Object[]{"foo", "shards", 1, "bar", "replicas", 2});
        execute("refresh table attrs");
        execute("select settings['routing']['allocation'] from information_schema.table_partitions where table_name='attrs'");
        Map<String, Object> routingAllocation = Map.ofEntries(
            Map.entry("enable", "all"),
            Map.entry("total_shards_per_node", 5),
            Map.entry("include", Map.of()),
            Map.entry("require", Map.of()),
            Map.entry("exclude", Map.of())
        );
        assertThat(response.rows()[0][0]).isEqualTo(routingAllocation);
        assertThat(response.rows()[1][0]).isEqualTo(routingAllocation);

        execute("alter table attrs set (\"routing.allocation.total_shards_per_node\"=1)");
        execute("alter table attrs PARTITION (name = 'foo') set (\"routing.allocation.exclude.foo\" = 'dummy')");
        execute("""
            select
                settings['routing']['allocation']
            from
                information_schema.table_partitions
            where
                table_name='attrs'
            order by partition_ident
            """);
        routingAllocation = Map.ofEntries(
            Map.entry("enable", "all"),
            Map.entry("total_shards_per_node", 1),
            Map.entry("include", Map.of()),
            Map.entry("require", Map.of()),
            Map.entry("exclude", Map.of())
        );
        assertThat(response.rows()[0][0]).isEqualTo(routingAllocation);
        routingAllocation = Map.ofEntries(
            Map.entry("enable", "all"),
            Map.entry("total_shards_per_node", 1),
            Map.entry("include", Map.of()),
            Map.entry("require", Map.of()),
            Map.entry("exclude", Map.of("foo", "dummy"))
        );
        assertThat(response.rows()[1][0]).isEqualTo(routingAllocation);
    }

    @Test
    public void testAlterPartitionedTableOnlySettings() throws Exception {
        execute("create table attrs (name string, attr string, value integer) " +
                "partitioned by (name) clustered into 1 shards with (number_of_replicas=0, \"routing.allocation.total_shards_per_node\"=5)");
        ensureYellow();
        execute("insert into attrs (name, attr, value) values (?, ?, ?), (?, ?, ?)",
            new Object[]{"foo", "shards", 1, "bar", "replicas", 2});
        execute("refresh table attrs");

        execute("alter table ONLY attrs set (\"routing.allocation.total_shards_per_node\"=1)");

        // setting is not changed for existing partitions
        execute("select settings['routing']['allocation']['total_shards_per_node'] from information_schema.table_partitions where table_name='attrs'");
        assertThat((Integer) response.rows()[0][0]).isEqualTo(5);
        assertThat((Integer) response.rows()[1][0]).isEqualTo(5);

        // new partitions must use new settings
        execute("insert into attrs (name, attr, value) values (?, ?, ?), (?, ?, ?)",
            new Object[]{"Arthur", "shards", 1, "Ford", "replicas", 2});
        execute("refresh table attrs");

        execute("select settings['routing']['allocation']['total_shards_per_node'] from information_schema.table_partitions where table_name='attrs' order by 1");
        assertThat((Integer) response.rows()[0][0]).isEqualTo(1);
        assertThat((Integer) response.rows()[1][0]).isEqualTo(1);
        assertThat((Integer) response.rows()[2][0]).isEqualTo(5);
        assertThat((Integer) response.rows()[3][0]).isEqualTo(5);
    }

    @Test
    public void testRefreshPartitionedTableAllPartitions() {
        execute("create table parted (id integer, name string, date timestamp with time zone) " +
            "partitioned by (date) with (refresh_interval=0)");

        execute("refresh table parted");
        assertThat(response.rowCount()).isIn(0L, -1L);

        execute("insert into parted (id, name, date) values " +
                "(1, 'Trillian', '1970-01-01'), " +
                "(2, 'Arthur', '1970-01-07')");
        assertThat(response.rowCount()).isEqualTo(2L);
        ensureYellow();

        // cannot tell what rows are visible
        // could be none, could be all
        execute("select count(*) from parted");
        // cannot exactly tell which rows are visible
        assertThat((Long) response.rows()[0][0]).isLessThanOrEqualTo(2L);

        execute("refresh table parted");
        assertThat(response.rowCount()).isEqualTo(2L);

        // assert that all is available after refresh
        execute("select count(*) from parted");
        assertThat((Long) response.rows()[0][0]).isEqualTo(2L);
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

        Asserts.assertSQLError(() -> execute("refresh table parted partition(date=0)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(NOT_FOUND, 4046)
            .hasMessageContaining(String.format("No partition for table '%s' with ident '04130' exists",
                                                              getFqn("parted")));
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
        assertThat(response.rowCount()).isEqualTo(2L);

        ensureYellow();
        execute("refresh table parted");
        assertThat(response.rowCount()).isEqualTo(2L);

        // assert that after refresh all columns are available
        execute("select * from parted");
        assertThat(response.rowCount()).isEqualTo(2L);

        execute("insert into parted (id, name, date) values " +
                "(3, 'Zaphod', '1970-01-01')," +
                "(4, 'Marvin', '1970-01-07')");
        assertThat(response.rowCount()).isEqualTo(2L);

        // cannot exactly tell which rows are visible
        execute("select * from parted");
        // cannot exactly tell how much rows are visible at this point
        assertThat(response.rowCount()).isLessThanOrEqualTo(4L);

        execute("refresh table parted PARTITION (date='1970-01-01')");
        assertThat(response.rowCount()).isEqualTo(1L);

        // assert all partition rows are available after refresh
        execute("select * from parted where date='1970-01-01'");
        assertThat(response.rowCount()).isEqualTo(2L);

        execute("refresh table parted PARTITION (date='1970-01-07')");
        assertThat(response.rowCount()).isEqualTo(1L);

        // assert all partition rows are available after refresh
        execute("select * from parted where date='1970-01-07'");
        assertThat(response.rowCount()).isEqualTo(2L);
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
        assertThat(response.rowCount()).isEqualTo(4L);

        execute("select * from t1 where age in (50, 90)");
        assertThat(response.rowCount()).isLessThanOrEqualTo(2L);

        execute("refresh table t1 partition (age=50, date='1970-01-07'), " +
                "              t1 partition (age=90, date='1970-01-01')");
        assertThat(response.rowCount()).isEqualTo(2L);

        execute("select * from t1 where age in (50, 90) and date in ('1970-01-07', '1970-01-01')");
        assertThat(response.rowCount()).isLessThanOrEqualTo(4L);
    }

    @Test
    public void testAlterPartitionedTableKeepsMetadata() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer, " +
                "  score double" +
                ") partitioned by (score) with (number_of_replicas=0, column_policy='dynamic')");
        execute("insert into dynamic_table (id, score) values (1, 10)");
        execute("refresh table dynamic_table");
        ensureYellow();

        DocTableInfo table = getTable("dynamic_table");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.partitionedBy()).containsExactly(ColumnIdent.of("score"));

        execute("alter table dynamic_table set (column_policy= 'dynamic')");
        waitNoPendingTasksOnAll();

        table = getTable("dynamic_table");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.partitionedBy()).containsExactly(ColumnIdent.of("score"));
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
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(0L);

        execute("insert into parted (id, name, date) values (1, 'Trillian', '1970-01-01'), (2, 'Ford', '2010-01-01')");
        ensureYellow();
        execute("refresh table parted");

        execute("select count(*) from parted");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void test_can_add_column_via_alter_table_on_partitioned_table() throws Exception {
        execute("create table t (id int primary key, date timestamp with time zone primary key) " +
                "partitioned by (date) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");

        if (randomBoolean()) {
            // presence of partition must not make a difference
            execute("insert into t (id, date) values (1, now())");
        }

        execute("alter table t add column name string");
        execute("alter table t add column ft_name string index using fulltext");

        execute("select * from t");
        assertThat(response).hasColumns("id", "date", "name", "ft_name");

        execute("show create table t");
        assertThat((String) response.rows()[0][0]).startsWith(
            """
            CREATE TABLE IF NOT EXISTS "doc"."t" (
               "id" INTEGER NOT NULL,
               "date" TIMESTAMP WITH TIME ZONE NOT NULL,
               "name" TEXT,
               "ft_name" TEXT INDEX USING FULLTEXT WITH (
                  analyzer = 'standard'
               ),
               PRIMARY KEY ("id", "date")
            )
            """.stripLeading()
        );

        // Verify that ADD COLUMN gets advanced OID.
        Schemas schemas = cluster().getMasterNodeInstance(NodeContext.class).schemas();
        DocTableInfo table = schemas.getTableInfo(RelationName.fromIndexName("t"));
        var dateRef = table.getReference(ColumnIdent.of("date"));
        var nameRef = table.getReference(ColumnIdent.of("name"));
        assertThat(nameRef.oid()).isGreaterThan(dateRef.oid());
    }

    @Test
    public void testInsertToPartitionFromQuery() throws Exception {
        this.setup.setUpLocations();
        execute("refresh table locations");

        execute("select name from locations order by id");
        assertThat(response.rowCount()).isEqualTo(13L);
        String firstName = (String) response.rows()[0][0];

        execute("create table locations_parted (" +
                " id string primary key," +
                " name string primary key," +
                " date timestamp with time zone" +
                ") clustered by(id) into 2 shards partitioned by(name) with(number_of_replicas=0)");
        ensureYellow();

        execute("insert into locations_parted (id, name, date) (select id, name, date from locations)");
        assertThat(response.rowCount()).isEqualTo(13L);

        execute("refresh table locations_parted");
        execute("select name from locations_parted order by id");
        assertThat(response.rowCount()).isEqualTo(13L);
        assertThat(response.rows()[0][0]).isEqualTo(firstName);
    }

    @Test
    public void testPartitionedTableNestedPk() throws Exception {
        execute("create table t (o object as (i int primary key, name string)) partitioned by (o['i']) with (number_of_replicas=0)");
        execute("insert into t (o) values (?)", new Object[]{Map.of("i", 1, "name", "Zaphod")});
        execute("refresh table t");
        execute("select o['i'], o['name'] from t");
        assertThat(response.rows()[0][0]).isEqualTo(1);
        execute("select distinct table_name, partition_ident from sys.shards where table_name = 't'");
        assertThat(printedTable(response.rows())).isEqualTo("t| 04132\n");
    }


    @Test
    public void testCreateTableWithIllegalCustomSchemaCheckedByES() {
        Asserts.assertSQLError(() -> execute("create table \"AA A\".t (" +
            "   name string," +
            "   d timestamp with time zone" +
            ") partitioned by (d) with (number_of_replicas=0)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4002)
            .hasMessageContaining("Relation name \"AA A.t\" is invalid.");
    }

    @Test
    public void testAlterNumberOfShards() throws Exception {
        execute("create table tbl (x int, p int) partitioned by (p) " +
                "clustered into 1 shards with (number_of_replicas = '0-all')");

        execute("select number_of_shards from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("1");

        execute("alter table tbl set (number_of_shards = 2)");

        execute("select number_of_shards from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("2");

        execute("insert into tbl (x, p) values (1, 1)");
        assertThat(response).hasRowCount(1);
        execute("refresh table tbl");

        execute("select number_of_shards from information_schema.table_partitions where table_name='tbl'");
        assertThat(response).hasRows("2");

        execute("alter table tbl set (number_of_shards = 1)");
        assertThat(execute("insert into tbl (x, p) values (2, 2)")).hasRowCount(1);
        ensureYellow();
        execute("refresh table tbl");

        execute("select number_of_replicas, number_of_shards from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("0-all| 1");

        execute("select values, number_of_shards from information_schema.table_partitions " +
                "where table_name = 'tbl' " +
                "order by number_of_shards ASC");
        assertThat(response).as("only new partition has updated number of shards").hasRows(
            "{p=2}| 1",
            "{p=1}| 2"
        );
    }

    @Test
    public void test_increase_number_of_shards_for_existing_partition() throws Exception {
        execute("CREATE TABLE tbl_parted (x int, p int) PARTITIONED BY(p) " +
            "CLUSTERED INTO 2 SHARDS WITH (number_of_replicas = 0)");

        execute("INSERT INTO tbl_parted(x, p) SELECT g, 1 FROM generate_series(1, 4, 1) AS g");
        execute("refresh table tbl_parted");

        execute("SELECT count(*) FROM sys.shards WHERE table_name = 'tbl_parted'");
        assertThat(response).hasRows("2");

        execute("ALTER TABLE tbl_parted PARTITION (p=1) SET (\"blocks.write\" = true)");
        execute("ALTER TABLE tbl_parted PARTITION (p=1) SET (number_of_shards = 4)");
        execute("ALTER TABLE tbl_parted PARTITION (p=1) SET (\"blocks.write\" = false)");

        execute("INSERT INTO tbl_parted(x, p) SELECT g, 1 FROM generate_series(5, 10, 1) AS g");
        execute("refresh table tbl_parted");

        execute("SELECT count(*) FROM sys.shards WHERE table_name = 'tbl_parted'");
        assertThat(response).hasRows("4");
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
        execute("select data['sessionid'] from event group by data['sessionid'] " +
                "order by format('%s', data['sessionid'])");
        assertThat(response.rows().length).isEqualTo(2);
        assertThat((String) response.rows()[0][0]).isEqualTo("hello");
        assertThat(response.rows()[1][0]).isNull();;
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
        execute("select data['sessionid'] from event where " +
                "format('%s', data['sessionid']) = 'ciao' order by data['sessionid']");
        assertThat(response.rows().length).isEqualTo(1);
        assertThat((String) response.rows()[0][0]).isEqualTo("ciao");
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
        execute("select data['sessionid'] from event order by data['sessionid'] ASC nulls first");
        assertThat(printedTable(response.rows())).isEqualTo(
            "NULL\n" +
            "ciao\n" +
            "hello\n");

        execute("select data['sessionid'] from event order by data['sessionid'] ASC nulls last");
        assertThat(printedTable(response.rows())).isEqualTo(
            "ciao\n" +
            "hello\n" +
            "NULL\n");

        execute("select data['sessionid'] from event order by data['sessionid'] DESC nulls first");
        assertThat(printedTable(response.rows())).isEqualTo(
            "NULL\n" +
            "hello\n" +
            "ciao\n");

        execute("select data['sessionid'] from event order by data['sessionid'] DESC nulls last");
        assertThat(printedTable(response.rows())).isEqualTo(
            "hello\n" +
            "ciao\n" +
            "NULL\n");
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

        execute("select data " +
                "from event " +
                "where data['sessionid'] is null " +
                "order by number");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(printedTable(response.rows())).isEqualTo(
            "{sessionid=NULL}\n" +
            "NULL\n");

        execute("select data " +
                "from event " +
                "where data['sessionid'] = 'ciao'");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("select data " +
                "from event " +
                "where data['sessionid'] in ('hello', 'goodbye') " +
                "order by number DESC");
        assertThat(printedTable(response.rows())).isEqualTo(
            "{sessionid=hello}\n");
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

        execute("select sessionid from event group by sessionid order by sessionid");
        assertThat(response.rows().length).isEqualTo(2);
        assertThat((String) response.rows()[0][0]).isEqualTo("hello");
        assertThat(response.rows()[1][0]).isNull();;
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
        assertThat(response.rows()[0][0]).isEqualTo(3L);
        execute("select count(*), job_id, arbitrary(name) from sys.operations_log where name='fetch' group by 2");
        assertThat(response.rowCount()).isLessThanOrEqualTo(1);
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

    @UseNewCluster
    @Test
    public void testPartitionedColumnIsNotIn_Raw() throws Exception {
        execute("create table t (p string primary key, v string) " +
                "partitioned by (p) " +
                "with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (p, v) values ('a', 'Marvin')");
        execute("refresh table t");
        execute("select _raw from t");
        assertThat(((String) response.rows()[0][0])).isEqualTo("{\"2\":\"Marvin\"}");
    }

    @Test
    public void testMatchPredicateOnPartitionedTableWithPKColumn() throws Throwable {
        execute("create table foo (id integer primary key, name string INDEX using fulltext) partitioned by (id)");
        ensureYellow();
        execute("insert into foo (id, name) values (?, ?)", new Object[]{1, "Marvin Other Name"});
        execute("insert into foo (id, name) values (?, ?)", new Object[]{2, "Ford Yet Another Name"});
        execute("refresh table foo");

        execute("select id from foo where match(name, 'Ford')");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(2);
    }

    @Test
    public void testMatchPredicateOnPartitionedTableWithoutPKColumn() throws Throwable {
        execute("create table foo (id integer, name string INDEX using fulltext) partitioned by (id)");
        ensureYellow();
        execute("insert into foo (id, name) values (?, ?)", new Object[]{1, "Marvin Other Name"});
        execute("insert into foo (id, name) values (?, ?)", new Object[]{2, "Ford Yet Another Name"});
        execute("refresh table foo");

        execute("select id from foo where match(name, 'Marvin')");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(1);
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
        assertThat((String) response.rows()[0][1]).isEqualTo("3");
        assertThat((String) response.rows()[1][1]).isEqualTo("0");
    }

    @Test
    public void testSelectByIdEmptyPartitionedTable() {
        execute("create table test (id integer, entity integer, primary key(id, entity))" +
                " partitioned by (entity) with (number_of_replicas=0)");
        ensureYellow();
        execute("select * from test where entity = 0 and id = 0");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testSelectByMultiIdEmptyPartitionedTable() {
        execute("create table test (id integer, entity integer, primary key(id, entity))" +
                " partitioned by (entity) with (number_of_replicas=0)");
        ensureYellow();
        execute("select * from test where entity = 0 and (id = 0 or id = 1)");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testScalarEvaluatesInErrorOnPartitionedTable() throws Exception {
        execute("create table t1 (id int) partitioned by (id) with (number_of_replicas=0)");
        ensureYellow();
        // we need at least 1 row/partition, otherwise the table is empty and no evaluation occurs
        execute("insert into t1 (id) values (1)");
        execute("refresh table t1");

        Asserts.assertSQLError(() -> execute("select id/0 from t1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("/ by zero");
    }

    @Test
    public void testSelectPartitionValueFromInformationSchema() throws Exception {
        execute("create table t1 (p int, obj object as (p int)) " +
                "partitioned by (p, obj['p']) " +
                "clustered into 1 shards");
        execute("insert into t1 (p, obj) values (1, {p=10})");

        execute("select values['p'], values['obj[''p'']'] from information_schema.table_partitions");
        assertThat(printedTable(response.rows())).isEqualTo("1| 10\n");
    }

    @Test
    public void testRefreshIgnoresClosedPartitions() {
        execute("create table t (x int, p int) " +
                "partitioned by (p) clustered into 1 shards with (number_of_replicas = 0, refresh_interval = 0)");
        execute("insert into t (x, p) values (1, 1), (2, 2)");
        execute("alter table t partition (p = 2) close");
        assertThat(execute("refresh table t").rowCount()).isEqualTo(1L);
        execute("select * from t");
        assertThat(response).hasRows("1| 1");
    }

    @Test
    public void test_refresh_not_existing_partition() {
        execute("CREATE TABLE doc.parted (x TEXT) PARTITIONED BY (x)");
        Asserts.assertSQLError(() -> execute("REFRESH TABLE doc.parted PARTITION (x = 'hddsGNJHSGFEFZÜ')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(NOT_FOUND, 4046)
            .hasMessageContaining("No partition for table 'doc.parted' with ident '048mgp34ed3ksii8ad3kcha6bb1po' exists");
    }

    @Test
    public void test_refresh_partition_in_non_partitioned_table() {
        execute("CREATE TABLE doc.not_parted (x TEXT)");
        Asserts.assertSQLError(() -> execute("REFRESH TABLE doc.not_parted PARTITION (x = 'n')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("table 'doc.not_parted' is not partitioned");
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
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("a| Marvin\n");
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
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("2\n");
    }

    @Test
    public void testOrderingOnPartitionColumn() {
        execute("create table t (x int, p int) partitioned by (p) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into t (p, x) values (1, 1), (1, 2), (2, 1)");
        execute("refresh table t");
        execute("select p, x from t order by p desc, x asc");
        assertThat(response).hasRows(
            "2| 1",
            "1| 1",
            "1| 2");
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
        execute("refresh table my_table");
        // verifying update
        execute("select content from my_table where par=2");
        assertThat(response).hasRowCount(3);

        // trying to perform an update on a partition with a write block
        Asserts.assertSQLError(() -> execute("update my_table set content=\'content42\' where par=1"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("blocked by: [FORBIDDEN/8/index write (api)];");
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

        Asserts.assertSQLError(() -> execute("insert into my_table (par, content) values (2, 'content42'), " +
                                   "(2, 'content42'), " +
                                   "(1, 'content2'), " +
                                   "(3, 'content6')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("blocked by: [FORBIDDEN/8/index write (api)];");

        execute("refresh table my_table");
        execute("select * from my_table");
        assertThat(response.rowCount()).isBetween(2L, 5L);
        //cleaning up
        execute("alter table my_table partition (par=1) set (\"blocks.write\"=false)");
    }

    @Test
    public void testCurrentVersionsAreSetOnPartitionCreation() throws Exception {
        execute("create table doc.p1 (id int, p int) partitioned by (p)");

        execute("insert into doc.p1 (id, p) values (1, 2)");
        execute("select version['created'] from information_schema.table_partitions where table_name='p1'");

        assertThat(response.rows()[0][0]).isEqualTo(Version.CURRENT.externalNumber());
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
        assertThat(response).hasRows(
            "2",  // match on p = 1
            "3",  // match on p = 1
            "4"); // match on id = 4
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
        execute("refresh table test");

        execute(
            "SELECT table_name, partition_ident, values " +
            "FROM information_schema.table_partitions " +
            "ORDER BY table_name, partition_ident");
        assertThat(
            printedTable(response.rows())).isEqualTo("test| 04732d1g64p36d9i60o30c1g| {metadata['date']=1401235200000}\n");
        assertThat(printedTable(execute("SELECT count(*) FROM test").rows())).isEqualTo("2\n");
    }

    @UseNewCluster
    @Test
    public void test_nested_partition_column_is_included_when_selecting_the_object_but_not_in_the_source() {
        execute("create table tbl (pk object as (id text, part text), primary key (pk['id'], pk['part'])) " +
                "partitioned by (pk['part'])");
        execute("insert into tbl (pk) values ({id='1', part='x'})");
        execute("insert into tbl (pk) (select {id='2', part='x'})");
        execute("refresh table tbl");
        execute("select _raw, pk, pk['id'], pk['part'] from tbl order by pk['id'] asc");
        assertThat(response).hasRows(
            "{\"1\":{\"2\":\"1\"}}| {id=1, part=x}| 1| x",
            "{\"1\":{\"2\":\"2\"}}| {id=2, part=x}| 2| x");

        execute("SELECT _raw, pk, pk['id'], pk['part'] FROM tbl " +
                " WHERE (pk['id'] = 1 AND pk['part'] = 'x') " +
                " OR    (pk['id'] = 2 AND pk['part'] = 'x') " +
                " ORDER BY pk['id'] ASC");
        assertThat(response).hasRows(
            "{\"1\":{\"2\":\"1\"}}| {id=1, part=x}| 1| x",
            "{\"1\":{\"2\":\"2\"}}| {id=2, part=x}| 2| x");
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_select_partitioned_by_column_with_query_then_fetch_plan() throws Exception {
        execute("create table doc.tbl (p int, ordinal int, name text) partitioned by (p)");
        execute("insert into doc.tbl (p, ordinal, name) values (1, 1, 'Arthur')");
        execute("insert into doc.tbl (p, ordinal, name) values (1, 2, 'Trillian')");
        execute("refresh table doc.tbl");

        execute("explain (costs false) select p, name from doc.tbl order by ordinal limit 100");
        assertThat(printedTable(response.rows())).isEqualTo(
            "Eval[p, name]\n" +
            "  └ Fetch[p, name, ordinal]\n" +
            "    └ Limit[100::bigint;0]\n" +
            "      └ OrderBy[ordinal ASC]\n" +
            "        └ Collect[doc.tbl | [_fetchid, ordinal] | true]\n"
        );
        execute("select p, name from doc.tbl order by ordinal limit 100");
        assertThat(printedTable(response.rows())).isEqualTo(
            "1| Arthur\n" +
            "1| Trillian\n"
        );
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void test_alter_table_drop_constraint_on_partitioned_table() throws Exception {
        // Dropping a constraint on a partitioned table used to fail before 5.1
        execute("create table t (id int primary key constraint check_id_gt_zero check(id > 0), " +
            "x int constraint check_x_gt_zero check(x > 0)) " +
            "partitioned by (id) " +
            "clustered into 1 shards " +
            "with (number_of_replicas=0)");
        String selectCheckConstraintStmt =
            "select table_schema, table_name, constraint_type, constraint_name " +
                "from information_schema.table_constraints " +
                "where table_name='t' and constraint_name = 'check_x_gt_zero' ";

        execute("alter table t drop constraint check_x_gt_zero");

        execute(selectCheckConstraintStmt);
        assertThat(response.rows()).isEmpty();
    }

    /**
     * This testcase tracks a bug: https://github.com/crate/crate/issues/13295
     * In short, number_of_replicas has two names 1) 'number_of_replicas' and 2) 'index.number_of_replicas'
     * One is for table parameter and the other is for index metadata. The bug was caused by the two being mixed up.
     */
    @Test
    public void test_set_number_of_replicas_on_partitioned_table() {
        execute("CREATE TABLE p_t (val int) PARTITIONED BY(val)");
        execute("INSERT INTO p_t VALUES (1),(2)");
        execute("ALTER TABLE p_t SET (\"number_of_replicas\" = '3')");
        execute("select number_of_replicas from information_schema.table_partitions");
        assertThat(printedTable(response.rows())).isEqualTo("3\n3\n");
    }

    @Test
    @UseJdbc(0) // Jdbc layer would convert timestamp
    public void test_can_select_casted_partitioned_column() throws Exception {
        execute("""
            CREATE TABLE tbl (
                ts TIMESTAMP,
                year TIMESTAMP GENERATED ALWAYS AS date_trunc('year',ts)
            ) PARTITIONED BY (year)
            """
        );
        execute("INSERT INTO tbl (ts) SELECT now()");
        execute("refresh table tbl");
        execute("select year, year::TEXT, ts from tbl LIMIT 10");
        assertThat(response.rows()[0][0].toString())
            .as("Column values must match: " + Arrays.toString(response.rows()[0]))
            .isEqualTo(response.rows()[0][1]);
    }

    @Test
    public void can_reuse_prepared_statement_to_select_from_partitioned_tables_with_newly_created_partition() throws Exception {
        execute("""
            CREATE TABLE doc.t (
                a int
            ) PARTITIONED BY (a);
            """);
        execute(
            """
                CREATE TABLE doc.t2 (
                    b int
                );
                """
        );
        // create a partition before creating a prepared statement
        execute("INSERT INTO doc.t VALUES (1)");
        execute("refresh table doc.t");

        try (var session = sqlExecutor.newSession()) {
            // create a prepared statement that selects from 'doc.t'
            session.parse(
                "preparedStatement",
                "INSERT INTO doc.t2 SELECT a FROM doc.t",
                List.of()
            );

            // create another partition for 'doc.t' after creating the prepared statement
            execute("INSERT INTO doc.t VALUES (2)");
            execute("refresh table doc.t");

            // execute the prepared statement
            session.bind("portalName", "preparedStatement", List.of(), null);
            session.execute("portalName", 0, new BaseResultReceiver());
            session.sync().get();
        }
        execute("refresh table doc.t2");

        // verify that '2' is inserted which implies that the prepared statement
        // can access the latest partition info and select from it
        execute("SELECT b FROM doc.t2 order by 1");
        assertThat(printedTable(response.rows())).isEqualTo("1\n2\n");
    }

    @Test
    public void selecting_doc_returns_partition_column_values() {
        execute("create table part (id integer, name string, date timestamp with time zone) partitioned by (name, date)");
        execute("insert into part (id, name, date) values (1, 'Ford', 0)");
        execute("insert into part (id, name, date) values (2, 'Trillian', 1)");
        execute("refresh table part");
        execute("select _doc from part order by id");
        assertThat(response.rows()[0][0]).isEqualTo(Map.of("date", "0", "id", 1, "name", "Ford"));
    }

    @Test
    public void nested_partition_column_in_doc() {
        execute("create table part (id integer, name string, obj object as (x string)) partitioned by (name, obj['x'])");
        execute("insert into part (id, name, obj) values (1, 'Ford', {x='a'})");
        execute("insert into part (id, name, obj) values (2, 'Trillian', {x='a'})");
        execute("refresh table part");
        execute("select _doc from part order by id");
        assertThat(response.rows()[0][0]).isEqualTo(Map.of("id", 1, "name", "Ford", "obj", Map.of("x", "a")));
    }
}
