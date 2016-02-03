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
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.*;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.reference.sys.cluster.ClusterNameExpression;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class HandlerSideLevelCollectTest extends SQLTransportIntegrationTest {

    private MapSideDataCollectOperation operation;
    private Functions functions;

    @Before
    public void prepare() {
        operation = internalCluster().getDataNodeInstance(MapSideDataCollectOperation.class);
        functions = internalCluster().getInstance(Functions.class);
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
                DistributionInfo.DEFAULT_BROADCAST
        );
    }

    private CollectPhase collectNode(Routing routing, List<Symbol> toCollect, RowGranularity rowGranularity) {
        return collectNode(routing, toCollect, rowGranularity, WhereClause.MATCH_ALL);
    }

    @Test
    public void testClusterLevel() throws Exception {
        Schemas schemas =  internalCluster().getInstance(Schemas.class);
        TableInfo tableInfo = schemas.getTableInfo(new TableIdent("sys", "cluster"));
        Routing routing = tableInfo.getRouting(WhereClause.MATCH_ALL, null);
        Reference clusterNameRef = new Reference(new ReferenceInfo(new ReferenceIdent(SysClusterTableInfo.IDENT, new ColumnIdent(ClusterNameExpression.NAME)), RowGranularity.CLUSTER, DataTypes.STRING));
        CollectPhase collectNode = collectNode(routing, Arrays.<Symbol>asList(clusterNameRef), RowGranularity.CLUSTER);
        Bucket result = collect(collectNode);
        assertThat(result.size(), is(1));
        assertThat(((BytesRef) result.iterator().next().get(0)).utf8ToString(), Matchers.startsWith("SUITE-"));
    }

    private Bucket collect(CollectPhase collectPhase) throws Exception {
        CollectingRowReceiver collectingProjector = new CollectingRowReceiver();
        collectingProjector.prepare(mock(ExecutionState.class));
        Collection<CrateCollector> collectors = operation.createCollectors(collectPhase, collectingProjector, mock(JobCollectContext.class));
        operation.launchCollectors(collectors, JobCollectContext.threadPoolName(collectPhase, clusterService().localNode().id()));
        return collectingProjector.result();
    }

    @Test
    public void testInformationSchemaTables() throws Exception {
        InformationSchemaInfo schemaInfo =  internalCluster().getInstance(InformationSchemaInfo.class);
        TableInfo tablesTableInfo = schemaInfo.getTableInfo("tables");
        Routing routing = tablesTableInfo.getRouting(WhereClause.MATCH_ALL, null);
        List<Symbol> toCollect = new ArrayList<>();
        for (ReferenceInfo info : tablesTableInfo.columns()) {
            toCollect.add(new Reference(info));
        }
        Symbol tableNameRef = toCollect.get(8);

        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME,
                ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(),
                Arrays.asList(tableNameRef, Literal.newLiteral("shards")));

        CollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC, new WhereClause(whereClause));
        Bucket result = collect(collectNode);
        assertThat(TestingHelpers.printedTable(result), is("NULL| NULL| strict| 0| 1| NULL| sys| NULL| shards\n"));
    }

    @Test
    public void testInformationSchemaColumns() throws Exception {
        InformationSchemaInfo schemaInfo =  internalCluster().getInstance(InformationSchemaInfo.class);
        TableInfo tableInfo = schemaInfo.getTableInfo("columns");
        assert tableInfo != null;
        Routing routing = tableInfo.getRouting(WhereClause.MATCH_ALL, null);
        List<Symbol> toCollect = new ArrayList<>();
        for (ReferenceInfo info : tableInfo.columns()) {
            toCollect.add(new Reference(info));
        }
        CollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC);
        Bucket result = collect(collectNode);

        String expected = "id| string| NULL| false| 1| sys| cluster\n" +
                          "master_node| string| NULL| false| 2| sys| cluster\n" +
                          "name| string| NULL| false| 3| sys| cluster\n" +
                          "settings| object| NULL| false| 4| sys| cluster\n";


        assertThat(TestingHelpers.printedTable(result), Matchers.containsString(expected));

        // second time - to check if the internal iterator resets
        result = collect(collectNode);
        assertThat(TestingHelpers.printedTable(result), Matchers.containsString(expected));
    }
}
