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

package io.crate.execution.engine.collect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.analyze.WhereClause;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.execution.jobs.JobSetup;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.TasksService;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.testing.UseRandomizedSchema;


@IntegTestCase.ClusterScope(numDataNodes = 1)
@UseRandomizedSchema(random = false)
public class DocLevelCollectTest extends IntegTestCase {

    private static final String TEST_TABLE_NAME = "test_table";
    private Reference testDocLevelReference;
    private Reference underscoreIdReference;

    private static final String PARTITIONED_TABLE_NAME = "parted_table";

    private Functions functions;
    private Schemas schemas;

    @Before
    public void prepare() {
        functions = cluster().getDataNodeInstance(Functions.class);
        schemas = cluster().getDataNodeInstance(NodeContext.class).schemas();

        execute(String.format(Locale.ENGLISH, "create table %s (" +
                                              "  id integer," +
                                              "  name string," +
                                              "  date timestamp with time zone" +
                                              ") clustered into 2 shards partitioned by (date, name) with(number_of_replicas=0)", PARTITIONED_TABLE_NAME));
        ensureGreen();
        execute(String.format(Locale.ENGLISH, "insert into %s (id, name, date) values (?, ?, ?)",
            PARTITIONED_TABLE_NAME),
            new Object[]{1, "Ford", 0L});
        execute(String.format(Locale.ENGLISH, "insert into %s (id, name, date) values (?, ?, ?)",
            PARTITIONED_TABLE_NAME),
            new Object[]{2, "Trillian", 1L});
        ensureGreen();
        refresh();

        execute(String.format(Locale.ENGLISH, "create table %s (" +
                                              " id integer primary key," +
                                              " doc integer" +
                                              ") clustered into 2 shards with(number_of_replicas=0)", TEST_TABLE_NAME));
        ensureGreen();
        execute(String.format(Locale.ENGLISH, "insert into %s (id, doc) values (?, ?)", TEST_TABLE_NAME), new Object[]{1, 2});
        execute(String.format(Locale.ENGLISH, "insert into %s (id, doc) values (?, ?)", TEST_TABLE_NAME), new Object[]{3, 4});
        refresh();

        var table = schemas.getTableInfo(RelationName.fromIndexName(TEST_TABLE_NAME));
        testDocLevelReference = table.getReference(ColumnIdent.fromPath("doc"));
        underscoreIdReference = table.getReference(ColumnIdent.fromPath("_id"));
    }

    @After
    public void cleanUp() {
        functions = null;
        schemas = null;
    }

    private Routing routing(String table) {
        Map<String, Map<String, IntIndexedContainer>> locations = new TreeMap<>();

        for (final ShardRouting shardRouting : clusterService().state().routingTable().allShards(table)) {
            Map<String, IntIndexedContainer> shardIds = locations.get(shardRouting.currentNodeId());
            if (shardIds == null) {
                shardIds = new TreeMap<>();
                locations.put(shardRouting.currentNodeId(), shardIds);
            }

            IntIndexedContainer shardIdSet = shardIds.get(shardRouting.getIndexName());
            if (shardIdSet == null) {
                shardIdSet = new IntArrayList();
                shardIds.put(shardRouting.index().getName(), shardIdSet);
            }
            shardIdSet.add(shardRouting.id());
        }
        return new Routing(locations);
    }

    @Test
    public void testCollectDocLevel() throws Throwable {
        List<Symbol> toCollect = Arrays.asList(testDocLevelReference, underscoreIdReference);
        RoutedCollectPhase collectNode = getCollectNode(toCollect, WhereClause.MATCH_ALL);
        Bucket result = collect(collectNode);
        assertThat(StreamSupport.stream(result.spliterator(), false).map(Row::materialize).toList())
            .containsExactlyInAnyOrder(
                new Object[] {2, "1"},
                new Object[] {4, "3"}
            );
    }

    @Test
    public void testCollectDocLevelWhereClause() throws Throwable {
        List<Symbol> arguments = Arrays.asList(testDocLevelReference, Literal.of(2));
        EqOperator op =
            (EqOperator) functions.get(null, EqOperator.NAME, arguments, SearchPath.pathWithPGCatalogAndDoc());
        List<Symbol> toCollect = Collections.singletonList(testDocLevelReference);
        WhereClause whereClause = new WhereClause(
            new Function(op.signature(), arguments, EqOperator.RETURN_TYPE)
        );
        RoutedCollectPhase collectNode = getCollectNode(toCollect, whereClause);

        Bucket result = collect(collectNode);
        assertThat(result).containsExactly(new Row1(2));
    }

    private RoutedCollectPhase getCollectNode(List<Symbol> toCollect, Routing routing, WhereClause whereClause) {
        return new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "docCollect",
            routing,
            RowGranularity.DOC,
            toCollect,
            List.of(),
            whereClause.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );
    }

    private RoutedCollectPhase getCollectNode(List<Symbol> toCollect, WhereClause whereClause) {
        return getCollectNode(toCollect, routing(TEST_TABLE_NAME), whereClause);
    }

    @Test
    public void testCollectWithPartitionedColumns() throws Throwable {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, PARTITIONED_TABLE_NAME);
        TableInfo tableInfo = schemas.getTableInfo(relationName);
        Routing routing = tableInfo.getRouting(
            clusterService().state(),
            new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList()),
            WhereClause.MATCH_ALL,
            RoutingProvider.ShardSelection.ANY,
            CoordinatorSessionSettings.systemDefaults());
        RoutedCollectPhase collectNode = getCollectNode(
            Arrays.asList(
                tableInfo.getReference(ColumnIdent.of("id")),
                tableInfo.getReference(ColumnIdent.of("date"))
            ),
            routing,
            WhereClause.MATCH_ALL
        );

        Bucket result = collect(collectNode);
        assertThat(StreamSupport.stream(result.spliterator(), false).map(Row::materialize).toList()).containsExactlyInAnyOrder(
            new Object[] {1, 0L},
            new Object[] {2, 1L}
        );
    }

    private Bucket collect(RoutedCollectPhase collectNode) throws Throwable {
        JobSetup jobSetup = cluster().getDataNodeInstance(JobSetup.class);
        TasksService tasksService = cluster().getDataNodeInstance(TasksService.class);
        SharedShardContexts sharedShardContexts = new SharedShardContexts(
            cluster().getDataNodeInstance(IndicesService.class), UnaryOperator.identity());
        RootTask.Builder builder = tasksService.newBuilder(collectNode.jobId());
        NodeOperation nodeOperation = NodeOperation.withDirectResponse(collectNode, mock(ExecutionPhase.class), (byte) 0,
            "remoteNode");

        List<CompletableFuture<StreamBucket>> results = jobSetup.prepareOnRemote(
            DUMMY_SESSION_INFO,
            List.of(nodeOperation),
            builder,
            sharedShardContexts
        );
        RootTask rootTask = tasksService.createTask(builder);
        rootTask.start();
        return results.get(0).get(2, TimeUnit.SECONDS);
    }
}
