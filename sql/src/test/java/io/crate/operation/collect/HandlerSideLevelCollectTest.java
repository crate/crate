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
import io.crate.core.collections.TreeMapBuilder;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.*;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.RowGranularity;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 1)
public class HandlerSideLevelCollectTest extends SQLTransportIntegrationTest {


    private MapSideDataCollectOperation operation;
    private Functions functions;
    private String localNodeId;


    @Before
    public void prepare() {
        operation = internalCluster().getDataNodeInstance(MapSideDataCollectOperation.class);
        functions = internalCluster().getInstance(Functions.class);
        localNodeId = internalCluster().getDataNodeInstance(ClusterService.class).state().nodes().localNodeId();
    }

    private CollectPhase collectNode(Routing routing,
                                     List<Symbol> toCollect,
                                     RowGranularity rowGranularity,
                                     WhereClause whereClause) {
        return new CollectPhase(
                UUID.randomUUID(),
                0,
                "dummy",
                routing,
                rowGranularity,
                toCollect,
                ImmutableList.<Projection>of(),
                whereClause,
                DistributionType.BROADCAST
        );
    }

    private CollectPhase collectNode(Routing routing, List<Symbol> toCollect, RowGranularity rowGranularity) {
        return collectNode(routing, toCollect, rowGranularity, WhereClause.MATCH_ALL);
    }

    @Test
    public void testClusterLevel() throws Exception {
        Routing routing = SysClusterTableInfo.ROUTING;
        Reference clusterNameRef = new Reference(SysClusterTableInfo.INFOS.get(new ColumnIdent("name")));
        CollectPhase collectNode = collectNode(routing, Arrays.<Symbol>asList(clusterNameRef), RowGranularity.CLUSTER);
        collectNode.handlerSideCollect(localNodeId);
        Bucket result = collect(collectNode);
        assertThat(result.size(), is(1));
        assertThat(((BytesRef) result.iterator().next().get(0)).utf8ToString(), Matchers.startsWith("SUITE-"));
    }

    private Bucket collect(CollectPhase collectNode) throws Exception {
        CollectingRowReceiver collectingProjector = new CollectingRowReceiver();
        collectingProjector.prepare(mock(ExecutionState.class));
        operation.collect(collectNode, collectingProjector, mock(JobCollectContext.class));
        return collectingProjector.result();
    }

    @Test
    public void testInformationSchemaTables() throws Exception {
        Routing routing = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder().put(
                TableInfo.NULL_NODE_ID, TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("information_schema.tables", null).map()
        ).map());
        InformationSchemaInfo schemaInfo =  internalCluster().getInstance(InformationSchemaInfo.class);
        TableInfo tablesTableInfo = schemaInfo.getTableInfo("tables");
        List<Symbol> toCollect = new ArrayList<>();
        for (ReferenceInfo info : tablesTableInfo.columns()) {
            toCollect.add(new Reference(info));
        }
        Symbol tableNameRef = toCollect.get(1);

        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME,
                ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(),
                Arrays.asList(tableNameRef, Literal.newLiteral("shards")));

        CollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC, new WhereClause(whereClause));
        collectNode.handlerSideCollect(localNodeId);
        Bucket result = collect(collectNode);
        System.out.println(TestingHelpers.printedTable(result));
        assertEquals("sys| shards| 1| 0| NULL| NULL| NULL| strict| NULL\n", TestingHelpers.printedTable(result));
    }

    @Test
    public void testInformationSchemaColumns() throws Exception {
        Routing routing = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder().put(
                TableInfo.NULL_NODE_ID, TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("information_schema.columns", null).map()
        ).map());
        InformationSchemaInfo schemaInfo =  internalCluster().getInstance(InformationSchemaInfo.class);
        TableInfo tableInfo = schemaInfo.getTableInfo("columns");
        List<Symbol> toCollect = new ArrayList<>();
        for (ReferenceInfo info : tableInfo.columns()) {
            toCollect.add(new Reference(info));
        }
        CollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC);
        collectNode.handlerSideCollect(localNodeId);
        Bucket result = collect(collectNode);


        String expected = "sys| cluster| id| 1| string\n" +
                "sys| cluster| name| 2| string\n" +
                "sys| cluster| master_node| 3| string\n" +
                "sys| cluster| settings| 4| object";


        assertTrue(TestingHelpers.printedTable(result).contains(expected));

        // second time - to check if the internal iterator resets
        System.out.println(TestingHelpers.printedTable(result));
        result = collect(collectNode);
        assertTrue(TestingHelpers.printedTable(result).contains(expected));
    }
}
