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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.job.ContextPreparer;
import io.crate.action.job.SharedShardContexts;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.*;
import io.crate.operation.NodeOperation;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;


@ESIntegTestCase.ClusterScope(randomDynamicTemplates = false, numDataNodes = 1)
public class DocLevelCollectTest extends SQLTransportIntegrationTest {

    private static final String TEST_TABLE_NAME = "test_table";
    private static final Reference testDocLevelReference = new Reference(
        new ReferenceIdent(new TableIdent(null, TEST_TABLE_NAME), "doc"),
        RowGranularity.DOC,
        DataTypes.INTEGER);
    private static final Reference underscoreIdReference = new Reference(
        new ReferenceIdent(new TableIdent(null, TEST_TABLE_NAME), "_id"),
        RowGranularity.DOC,
        DataTypes.STRING);
    private static final Reference underscoreRawReference = new Reference(
        new ReferenceIdent(new TableIdent(null, TEST_TABLE_NAME), "_raw"),
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
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();

        for (final ShardRouting shardRouting : clusterService().state().routingTable().allShards(table)) {
            Map<String, List<Integer>> shardIds = locations.get(shardRouting.currentNodeId());
            if (shardIds == null) {
                shardIds = new TreeMap<>();
                locations.put(shardRouting.currentNodeId(), shardIds);
            }

            List<Integer> shardIdSet = shardIds.get(shardRouting.index());
            if (shardIdSet == null) {
                shardIdSet = new ArrayList<>();
                shardIds.put(shardRouting.index(), shardIdSet);
            }
            shardIdSet.add(shardRouting.id());
        }
        return new Routing(locations);
    }

    @Test
    public void testCollectDocLevel() throws Throwable {
        List<Symbol> toCollect = Arrays.<Symbol>asList(testDocLevelReference, underscoreRawReference, underscoreIdReference);
        RoutedCollectPhase collectNode = getCollectNode(toCollect, WhereClause.MATCH_ALL);
        Bucket result = collect(collectNode);
        assertThat(result, containsInAnyOrder(
            isRow(2, "{\"id\":1,\"doc\":2}", "1"),
            isRow(4, "{\"id\":3,\"doc\":4}", "3")
        ));
    }

    @Test
    public void testCollectDocLevelWhereClause() throws Throwable {
        EqOperator op = (EqOperator) functions.get(new FunctionIdent(EqOperator.NAME,
            ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.INTEGER)));
        List<Symbol> toCollect = Collections.<Symbol>singletonList(testDocLevelReference);
        WhereClause whereClause = new WhereClause(new Function(
            op.info(),
            Arrays.<Symbol>asList(testDocLevelReference, Literal.of(2)))
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
            ImmutableList.<Projection>of(),
            whereClause,
            DistributionInfo.DEFAULT_BROADCAST,
            (byte) 0);
    }

    private RoutedCollectPhase getCollectNode(List<Symbol> toCollect, WhereClause whereClause) {
        return getCollectNode(toCollect, routing(TEST_TABLE_NAME), whereClause);
    }

    @Test
    public void testCollectWithPartitionedColumns() throws Throwable {
        TableIdent tableIdent = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, PARTITIONED_TABLE_NAME);
        Routing routing = schemas.getTableInfo(tableIdent).getRouting(WhereClause.MATCH_ALL, null);
        RoutedCollectPhase collectNode = getCollectNode(
            Arrays.<Symbol>asList(
                new Reference(new ReferenceIdent(tableIdent, "id"),
                    RowGranularity.DOC,
                    DataTypes.INTEGER),
                new Reference(new ReferenceIdent(tableIdent, "date"),
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
        ContextPreparer contextPreparer = internalCluster().getDataNodeInstance(ContextPreparer.class);
        JobContextService contextService = internalCluster().getDataNodeInstance(JobContextService.class);
        SharedShardContexts sharedShardContexts = new SharedShardContexts(internalCluster().getDataNodeInstance(IndicesService.class));
        JobExecutionContext.Builder builder = contextService.newBuilder(collectNode.jobId());
        NodeOperation nodeOperation = NodeOperation.withDownstream(collectNode, mock(ExecutionPhase.class), (byte) 0,
            "remoteNode");

        List<ListenableFuture<Bucket>> results = contextPreparer.prepareOnRemote(
            ImmutableList.of(nodeOperation), builder, sharedShardContexts);
        JobExecutionContext context = contextService.createContext(builder);
        context.start();
        return results.get(0).get(2, TimeUnit.SECONDS);
    }
}
