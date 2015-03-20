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
import io.crate.analyze.WhereClause;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;


@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class DocLevelCollectTest extends SQLTransportIntegrationTest {
    private static final String TEST_TABLE_NAME = "test_table";
    private static final Reference testDocLevelReference = new Reference(
            new ReferenceInfo(
                    new ReferenceIdent(new TableIdent(null, TEST_TABLE_NAME), "doc"),
                    RowGranularity.DOC,
                    DataTypes.INTEGER
            )
    );
    private static final Reference underscoreIdReference = new Reference(
            new ReferenceInfo(
                    new ReferenceIdent(new TableIdent(null, TEST_TABLE_NAME), "_id"),
                    RowGranularity.DOC,
                    DataTypes.STRING
            )
    );
    private static final Reference underscoreRawReference = new Reference(
            new ReferenceInfo(
                    new ReferenceIdent(new TableIdent(null, TEST_TABLE_NAME), "_raw"),
                    RowGranularity.DOC,
                    DataTypes.STRING
            )
    );

    private static final String PARTITIONED_TABLE_NAME = "parted_table";

    private NonDistributingCollectOperation operation;
    private Functions functions;
    private DocSchemaInfo docSchemaInfo;

    @Before
    public void prepare() {
        operation = cluster().getInstance(NonDistributingCollectOperation.class);
        functions = cluster().getInstance(Functions.class);
        docSchemaInfo = cluster().getInstance(DocSchemaInfo.class);

        execute(String.format(Locale.ENGLISH, "create table %s (" +
                "  id integer," +
                "  name string," +
                "  date timestamp" +
                ") clustered into 2 shards partitioned by (date) with(number_of_replicas=0)", PARTITIONED_TABLE_NAME));
        ensureGreen();
        execute(String.format("insert into %s (id, name, date) values (?, ?, ?)",
                PARTITIONED_TABLE_NAME),
                new Object[]{1, "Ford", 0L});
        execute(String.format("insert into %s (id, name, date) values (?, ?, ?)",
                PARTITIONED_TABLE_NAME),
                new Object[]{2, "Trillian", 1L});
        ensureGreen();
        refresh();

        execute(String.format(Locale.ENGLISH, "create table %s (" +
                " id integer primary key," +
                " doc integer" +
                ") clustered into 2 shards with(number_of_replicas=0)", TEST_TABLE_NAME));
        ensureGreen();
        execute(String.format("insert into %s (id, doc) values (?, ?)", TEST_TABLE_NAME), new Object[]{1, 2});
        execute(String.format("insert into %s (id, doc) values (?, ?)", TEST_TABLE_NAME), new Object[]{3, 4});
        refresh();
    }

    @After
    public void cleanUp() {
        operation = null;
        functions = null;
        docSchemaInfo = null;
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
    public void testCollectDocLevel() throws Exception {
        CollectNode collectNode = new CollectNode("docCollect", routing(TEST_TABLE_NAME));
        collectNode.toCollect(Arrays.<Symbol>asList(testDocLevelReference, underscoreRawReference, underscoreIdReference));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.jobId(UUID.randomUUID());
        PlanNodeBuilder.setOutputTypes(collectNode);
        Bucket result = collect(collectNode);
        assertThat(result, contains(
                isRow(2, "{\"id\":1,\"doc\":2}", "1"),
                isRow(4, "{\"id\":3,\"doc\":4}", "3")
        ));
    }

    @Test
    public void testCollectDocLevelWhereClause() throws Exception {
        EqOperator op = (EqOperator) functions.get(new FunctionIdent(EqOperator.NAME,
                ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.INTEGER)));
        CollectNode collectNode = new CollectNode("docCollect", routing(TEST_TABLE_NAME));
        collectNode.jobId(UUID.randomUUID());
        collectNode.toCollect(Arrays.<Symbol>asList(testDocLevelReference));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.whereClause(new WhereClause(new Function(
                op.info(),
                Arrays.<Symbol>asList(testDocLevelReference, Literal.newLiteral(2)))
        ));
        PlanNodeBuilder.setOutputTypes(collectNode);

        Bucket result = collect(collectNode);
        assertThat(result, contains(isRow(2)));
    }


    @Test
    public void testCollectWithPartitionedColumns() throws Exception {
        Routing routing = docSchemaInfo.getTableInfo(PARTITIONED_TABLE_NAME).getRouting(WhereClause.MATCH_ALL);
        TableIdent tableIdent = new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, PARTITIONED_TABLE_NAME);
        CollectNode collectNode = new CollectNode("docCollect", routing);
        collectNode.toCollect(Arrays.<Symbol>asList(
                new Reference(new ReferenceInfo(
                        new ReferenceIdent(tableIdent, "id"),
                        RowGranularity.DOC, DataTypes.INTEGER)),
                new Reference(new ReferenceInfo(
                        new ReferenceIdent(tableIdent, "date"),
                        RowGranularity.SHARD, DataTypes.TIMESTAMP))
        ));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.isPartitioned(true);
        collectNode.jobId(UUID.randomUUID());
        PlanNodeBuilder.setOutputTypes(collectNode);

        Bucket result = collect(collectNode);
        for (Row row : result) {
            System.out.println("Row:" + Arrays.toString(row.materialize()));
        }

        assertThat(result, containsInAnyOrder(
                isRow(1, 0L),
                isRow(2, 1L)
        ));
    }

    private Bucket collect(CollectNode collectNode) throws InterruptedException, java.util.concurrent.ExecutionException {
        return operation.collect(collectNode, null).get();
    }
}
