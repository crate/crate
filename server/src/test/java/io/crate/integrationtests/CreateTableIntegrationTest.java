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
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.Asserts;

public class CreateTableIntegrationTest extends IntegTestCase {

    @Test
    public void testCreateTableIfNotExistsConcurrently() throws Throwable {
        executeCreateTableThreaded("create table if not exists t (name string) with (number_of_replicas = 0)");
    }

    @Test
    public void testCreatePartitionedTableIfNotExistsConcurrently() throws Throwable {
        executeCreateTableThreaded("create table if not exists t " +
                                   "(name string, p string) partitioned by (p) " +
                                   "with (number_of_replicas = 0)");
    }

    @Test
    public void test_allow_drop_of_corrupted_table() throws Exception {
        execute("create table doc.tbl (ts timestamp without time zone as '2020-01-01')");
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("default")
                .startObject("_meta")
                    .startObject("generated_columns")
                        .field("ts", "foobar")
                    .endObject()
                .endObject()
            .endObject()
            .endObject();

        ClusterService clusterService = cluster().getCurrentMasterNodeInstance(ClusterService.class);
        IndicesService indicesService = cluster().getCurrentMasterNodeInstance(IndicesService.class);
        var updateTask = new AckedClusterStateUpdateTask<AcknowledgedResponse>(new AcknowledgedRequest() {}) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Metadata metadata = currentState.metadata();
                IndexMetadata indexMetadata = metadata.index("tbl");
                MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
                mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);

                BytesReference newMapping = BytesReference.bytes(builder);
                DocumentMapper newMapper = mapperService.merge(new CompressedXContent(newMapping), MergeReason.MAPPING_UPDATE);
                return ClusterState.builder(currentState)
                    .metadata(
                        Metadata.builder(metadata)
                            .put(
                                IndexMetadata.builder(indexMetadata)
                                    .putMapping(new MappingMetadata(newMapper.mappingSource()))
                                    .mappingVersion(indexMetadata.getMappingVersion() + 1)
                            )
                    )
                    .build();
                }
        };
        clusterService.submitStateUpdateTask("corrupt-mapping", updateTask);
        assertThat(updateTask.completionFuture()).succeedsWithin(5, TimeUnit.SECONDS);

        assertThat(cluster().getInstance(ClusterService.class).state().metadata().hasIndex("tbl"))
            .isTrue();
        Asserts.assertSQLError(() -> execute("select * from doc.tbl"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'doc.tbl' unknown");
        execute("drop table doc.tbl");
        execute("select count(*) from information_schema.tables where table_name = 'tbl'");
        assertThat(response.rows()[0][0]).isEqualTo(0L);
        assertThat(cluster().getInstance(ClusterService.class).state().metadata().hasIndex("tbl"))
            .isFalse();
    }

    private void executeCreateTableThreaded(final String statement) throws Throwable {
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();

        for (int i = 0; i < 20; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        execute(statement);
                    } catch (Throwable t) {
                        lastThrowable.set(t);
                    }
                }
            });
        }

        executorService.shutdown();
        assertThat(executorService.awaitTermination(10, TimeUnit.SECONDS))
            .as("executorservice did not shutdown within timeout")
            .isTrue();

        Throwable throwable = lastThrowable.get();
        if (throwable != null) {
            throw throwable;
        }
    }

    /**
     * https://github.com/crate/crate/issues/12196
     */
    @Test
    public void test_intervals_in_generated_column() {
        execute("create table test(t timestamp, calculated timestamp generated always as DATE_BIN('15 minutes'::INTERVAL, t, '2001-01-01'))");
        execute("insert into test(t) values('2020-02-11 15:44:17')");
        refresh();
        execute("select date_format('%Y-%m-%d %H:%i:%s', calculated) from test");
        assertThat(printedTable(response.rows())).isEqualTo("2020-02-11 15:30:00\n");
    }

    @Test
    public void test_enforce_soft_deletes() {
        Asserts.assertSQLError(
            () -> execute("create table test(t timestamp) with (\"soft_deletes.enabled\" = false)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Creating tables with soft-deletes disabled is no longer supported.");

    }

    public void test_enforce_soft_deletes_on_partitioned_tables() {
        Asserts.assertSQLError(
            () -> execute("create table test(t timestamp) partitioned by (t) with (\"soft_deletes.enabled\" = false)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Creating tables with soft-deletes disabled is no longer supported.");

    }

    @Test
    public void test_constraint_on_generated_column() {
        execute(
            """
                CREATE TABLE test(
                    col1 INT,
                    col2 INT GENERATED ALWAYS AS col1*2 CONSTRAINT gt_zero CHECK (col2 > 0)
                )
                """);
        Asserts.assertSQLError(
            () -> execute("INSERT INTO test(col1) VALUES(0)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Failed CONSTRAINT gt_zero CHECK (\"col2\" > 0) for values: [0]");

        execute("INSERT INTO test(col1) VALUES(1),(2),(3)");
        assertThat(printedTable(response.rows()))
            .isEqualTo("");
    }

    @Test
    public void test_column_positions_from_create_table() {
        execute(
            """
                create table t (
                    ta boolean default true,
                    tb text constraint c check (length(tb) > 3),
                    tc object(dynamic) as (
                        td text,
                        te timestamp with time zone,
                        tf object(dynamic) as (
                            tg integer
                        )
                    ),
                    INDEX th using fulltext(tb, tc['td']),
                    ti ARRAY(OBJECT AS (tj INTEGER, tk object as (tl text))),
                    tm ARRAY(GEO_POINT),
                    tn boolean as (ta = false)
                )
                """
        );
        execute("""
                    select column_name, ordinal_position
                    from information_schema.columns
                    where table_name = 't'
                    order by 2""");
        assertThat(response).hasRows(
             "ta| 1",
             "tb| 2",
             "tc| 3",
             "tc['td']| 4",
             "tc['te']| 5",
             "tc['tf']| 6",
             "tc['tf']['tg']| 7",
             "ti| 9",
             "ti['tj']| 10",
             "ti['tk']| 11",
             "ti['tk']['tl']| 12",
             "tm| 13",
             "tn| 14"
        ); // 'th' is a named index and is assigned column position 8

        execute("select * from t");
        assertThat(response.cols()).isEqualTo(new String[] {"ta", "tb", "tc", "ti", "tm", "tn"});
    }
}
