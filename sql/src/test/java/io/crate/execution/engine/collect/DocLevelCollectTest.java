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

package io.crate.execution.engine.collect;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;
import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.data.Bucket;
import io.crate.data.Row;
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
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.testing.UseRandomizedSchema;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;


@ESIntegTestCase.ClusterScope(numDataNodes = 1)
@UseRandomizedSchema(random = false)
public class DocLevelCollectTest extends SQLTransportIntegrationTest {

    private static final String TEST_TABLE_NAME = "test_table";
    private static final Reference testDocLevelReference = new Reference(
        new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, TEST_TABLE_NAME), "doc"),
        RowGranularity.DOC,
        DataTypes.INTEGER);
    private static final Reference underscoreIdReference = new Reference(
        new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, TEST_TABLE_NAME), "_id"),
        RowGranularity.DOC,
        DataTypes.STRING);
    private static final Reference underscoreRawReference = new Reference(
        new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, TEST_TABLE_NAME), "_raw"),
        RowGranularity.DOC,
        DataTypes.STRING);

    private static final String PARTITIONED_TABLE_NAME = "parted_table";

    private Functions functions;
    private Schemas schemas;

    @Before
    public void prepare() {
        functions = internalCluster().getDataNodeInstance(Functions.class);
        schemas = internalCluster().getDataNodeInstance(Schemas.class);

        execute(String.format(Locale.ENGLISH, "create table %s (" +
                                              "  id integer," +
                                              "  name string," +
                                              "  date timestamp" +
                                              ") clustered into 2 shards partitioned by (date) with(number_of_replicas=0)", PARTITIONED_TABLE_NAME));
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
        assertThat(result, containsInAnyOrder(
            isRow(2, "1"),
            isRow(4, "3")
        ));
    }

    @Test
    public void testCollectDocLevelWhereClause() throws Throwable {
        List<Symbol> arguments = Arrays.asList(testDocLevelReference, Literal.of(2));
        EqOperator op =
            (EqOperator) functions.get(null, EqOperator.NAME, arguments, SearchPath.pathWithPGCatalogAndDoc());
        List<Symbol> toCollect = Collections.singletonList(testDocLevelReference);
        WhereClause whereClause = new WhereClause(new Function(
            op.info(),
            arguments)
        );
        RoutedCollectPhase collectNode = getCollectNode(toCollect, whereClause);

        Bucket result = collect(collectNode);
        assertThat(result, contains(isRow(2)));
    }

    private RoutedCollectPhase getCollectNode(List<Symbol> toCollect, Routing routing, WhereClause whereClause) {
        return new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "docCollect",
            routing,
            RowGranularity.DOC,
            toCollect,
            ImmutableList.of(),
            whereClause.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
    }

    private RoutedCollectPhase getCollectNode(List<Symbol> toCollect, WhereClause whereClause) {
        return getCollectNode(toCollect, routing(TEST_TABLE_NAME), whereClause);
    }

    @Test
    public void testCollectWithPartitionedColumns() throws Throwable {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, PARTITIONED_TABLE_NAME);
        Routing routing = schemas.getTableInfo(relationName).getRouting(
            clusterService().state(),
            new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList()),
            WhereClause.MATCH_ALL,
            RoutingProvider.ShardSelection.ANY,
            SessionContext.systemSessionContext());
        RoutedCollectPhase collectNode = getCollectNode(
            Arrays.asList(
                new Reference(new ReferenceIdent(relationName, "id"),
                    RowGranularity.DOC,
                    DataTypes.INTEGER),
                new Reference(new ReferenceIdent(relationName, "date"),
                    RowGranularity.SHARD,
                    DataTypes.TIMESTAMP)),
            routing,
            WhereClause.MATCH_ALL
        );

        Bucket result = collect(collectNode);
        for (Row row : result) {
            System.out.println("Row:" + Arrays.toString(row.materialize()));
        }

        assertThat(result, containsInAnyOrder(
            isRow(1, 0L),
            isRow(2, 1L)
        ));
    }

    private Bucket collect(RoutedCollectPhase collectNode) throws Throwable {
        JobSetup jobSetup = internalCluster().getDataNodeInstance(JobSetup.class);
        TasksService tasksService = internalCluster().getDataNodeInstance(TasksService.class);
        SharedShardContexts sharedShardContexts = new SharedShardContexts(
            internalCluster().getDataNodeInstance(IndicesService.class), UnaryOperator.identity());
        RootTask.Builder builder = tasksService.newBuilder(collectNode.jobId());
        NodeOperation nodeOperation = NodeOperation.withDirectResponse(collectNode, mock(ExecutionPhase.class), (byte) 0,
            "remoteNode");

        List<CompletableFuture<StreamBucket>> results = jobSetup.prepareOnRemote(
            ImmutableList.of(nodeOperation), builder, sharedShardContexts);
        RootTask rootTask = tasksService.createTask(builder);
        rootTask.start();
        return results.get(0).get(2, TimeUnit.SECONDS);
    }
}
