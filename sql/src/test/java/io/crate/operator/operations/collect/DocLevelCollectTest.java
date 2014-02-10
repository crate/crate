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

package io.crate.operator.operations.collect;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operator.operator.EqOperator;
import io.crate.operator.reference.sys.node.NodeNameExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.IntegerLiteral;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.collection.IsIn.isIn;
import static org.hamcrest.collection.IsIn.isOneOf;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class DocLevelCollectTest extends SQLTransportIntegrationTest {
    private static final String TEST_TABLE_NAME = "test_table";
    private static final Reference testDocLevelReference = new Reference(
            new ReferenceInfo(
                    new ReferenceIdent(new TableIdent(null, TEST_TABLE_NAME), "doc"),
                    RowGranularity.DOC,
                    DataType.INTEGER
            )
    );

    private LocalDataCollectOperation operation;
    private Functions functions;

    @Before
    public void prepare() {
        Loggers.getLogger(LocalDataCollectOperation.class).setLevel("TRACE");
        operation = cluster().getInstance(LocalDataCollectOperation.class);
        functions = cluster().getInstance(Functions.class);

        execute(String.format("create table %s (" +
                " id integer primary key," +
                " doc integer" +
                ") clustered into 2 shards replicas 0", TEST_TABLE_NAME));
        ensureGreen();
        execute(String.format("insert into %s (id, doc) values (?, ?)", TEST_TABLE_NAME), new Object[]{1, 2});
        execute(String.format("insert into %s (id, doc) values (?, ?)", TEST_TABLE_NAME), new Object[]{3, 4});
        refresh();
    }

    private Routing routing() {
        Map<String, Map<String, Set<Integer>>> locations = new HashMap<>();

        for (final ShardRouting shardRouting : clusterService().state().routingTable().allShards(TEST_TABLE_NAME)) {
            Map<String, Set<Integer>> shardIds = locations.get(shardRouting.currentNodeId());
            if (shardIds == null) {
                shardIds = new HashMap<>();
                locations.put(shardRouting.currentNodeId(), shardIds);
            }

            Set<Integer> shardIdSet = shardIds.get(shardRouting.index());
            if (shardIdSet == null) {
                shardIdSet = new HashSet<>();
                shardIds.put(shardRouting.index(), shardIdSet);
            }
            shardIdSet.add(shardRouting.id());

        }
        return new Routing(locations);
    }

    @Test
    public void testCollectDocLevel() throws Exception {
        CollectNode collectNode = new CollectNode("docCollect", routing());
        collectNode.toCollect(Arrays.<Symbol>asList(testDocLevelReference));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        Object[][] result = operation.collect(collectNode).get();
        assertThat(result.length, is(2));
        assertThat(result[0].length, is(1));
        assertThat((Integer) result[0][0], isOneOf(2, 4));
        assertThat((Integer) result[1][0], isOneOf(2, 4));
    }

    @Test
    public void testCollectDocLevelWhereClause() throws Exception {
        EqOperator op = (EqOperator) functions.get(new FunctionIdent(EqOperator.NAME, ImmutableList.of(DataType.INTEGER, DataType.INTEGER)));
        CollectNode collectNode = new CollectNode("docCollect", routing());
        collectNode.toCollect(Arrays.<Symbol>asList(testDocLevelReference));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.whereClause(new Function(
                op.info(),
                Arrays.<Symbol>asList(testDocLevelReference, new IntegerLiteral(2))
        ));

        Object[][] result = operation.collect(collectNode).get();
        assertThat(result.length, is(1));
        assertThat(result[0].length, is(1));
        assertThat((Integer) result[0][0], is(2));
    }

    @Test
    public void testCollectWithShardAndNodeExpressions() throws Exception {
        Routing routing = routing();
        Set shardIds = routing.locations().get(clusterService().localNode().id()).get(TEST_TABLE_NAME);

        CollectNode collectNode = new CollectNode("docCollect", routing);
        collectNode.toCollect(Arrays.<Symbol>asList(
                testDocLevelReference,
                new Reference(NodeNameExpression.INFO_NAME),
                new Reference(SysShardsTableInfo.INFOS.get(new ColumnIdent("id"))),
                new Reference(SysClusterTableInfo.INFOS.get(new ColumnIdent("name")))
        ));
        collectNode.maxRowGranularity(RowGranularity.DOC);

        Object[][] result = operation.collect(collectNode).get();

        assertThat(result.length, is(2));
        assertThat(result[0].length, is(4));
        assertThat((Integer) result[0][0], isOneOf(2, 4));
        assertThat(((BytesRef) result[0][1]).utf8ToString(), is(clusterService().localNode().name()));
        assertThat(result[0][2], isIn(shardIds));
        assertThat((String) result[0][3], is(cluster().clusterName()));

        assertThat((Integer) result[1][0], isOneOf(2, 4));
        assertThat(((BytesRef) result[1][1]).utf8ToString(), is(clusterService().localNode().name()));
        assertThat(result[1][2], isIn(shardIds));

        assertThat((String) result[1][3], is(cluster().clusterName()));
    }
}
