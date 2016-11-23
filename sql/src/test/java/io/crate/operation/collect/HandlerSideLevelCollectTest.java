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
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.reference.sys.cluster.ClusterNameExpression;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
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

    private RoutedCollectPhase collectNode(Routing routing,
                                           List<Symbol> toCollect,
                                           RowGranularity rowGranularity,
                                           WhereClause whereClause) {
        return new RoutedCollectPhase(
            UUID.randomUUID(),
            0,
            "dummy",
            routing,
            rowGranularity,
            toCollect,
            ImmutableList.<Projection>of(),
            whereClause,
            DistributionInfo.DEFAULT_BROADCAST,
            (byte) 0);
    }

    private RoutedCollectPhase collectNode(Routing routing, List<Symbol> toCollect, RowGranularity rowGranularity) {
        return collectNode(routing, toCollect, rowGranularity, WhereClause.MATCH_ALL);
    }

    @Test
    public void testClusterLevel() throws Exception {
        Schemas schemas = internalCluster().getInstance(Schemas.class);
        TableInfo tableInfo = schemas.getTableInfo(new TableIdent("sys", "cluster"));
        Routing routing = tableInfo.getRouting(WhereClause.MATCH_ALL, null);
        Reference clusterNameRef = new Reference(new ReferenceIdent(SysClusterTableInfo.IDENT, new ColumnIdent(ClusterNameExpression.NAME)), RowGranularity.CLUSTER, DataTypes.STRING);
        RoutedCollectPhase collectNode = collectNode(routing, Arrays.<Symbol>asList(clusterNameRef), RowGranularity.CLUSTER);
        Bucket result = collect(collectNode);
        assertThat(result.size(), is(1));
        assertThat(((BytesRef) result.iterator().next().get(0)).utf8ToString(), Matchers.startsWith("SUITE-"));
    }

    private Bucket collect(RoutedCollectPhase collectPhase) throws Exception {
        CollectingRowReceiver collectingProjector = new CollectingRowReceiver();
        Collection<CrateCollector> collectors = operation.createCollectors(collectPhase, collectingProjector, mock(JobCollectContext.class));
        operation.launchCollectors(collectors, JobCollectContext.threadPoolName(collectPhase, clusterService().localNode().id()));
        return collectingProjector.result();
    }

    @Test
    public void testInformationSchemaTables() throws Exception {
        InformationSchemaInfo schemaInfo = internalCluster().getInstance(InformationSchemaInfo.class);
        TableInfo tablesTableInfo = schemaInfo.getTableInfo("tables");
        Routing routing = tablesTableInfo.getRouting(WhereClause.MATCH_ALL, null);
        List<Symbol> toCollect = new ArrayList<>();
        for (Reference reference : tablesTableInfo.columns()) {
            toCollect.add(reference);
        }
        Symbol tableNameRef = toCollect.get(7);

        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME,
            ImmutableList.of(DataTypes.STRING, DataTypes.STRING)));
        Function whereClause = new Function(eqImpl.info(),
            Arrays.asList(tableNameRef, Literal.of("shards")));

        RoutedCollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC, new WhereClause(whereClause));
        Bucket result = collect(collectNode);
        assertThat(TestingHelpers.printedTable(result), is("NULL| NULL| strict| 0| 1| NULL| NULL| shards| sys\n"));
    }

    @Test
    public void testInformationSchemaColumns() throws Exception {
        InformationSchemaInfo schemaInfo = internalCluster().getInstance(InformationSchemaInfo.class);
        TableInfo tableInfo = schemaInfo.getTableInfo("columns");
        assert tableInfo != null;
        Routing routing = tableInfo.getRouting(WhereClause.MATCH_ALL, null);
        List<Symbol> toCollect = new ArrayList<>();
        for (Reference ref : tableInfo.columns()) {
            toCollect.add(ref);
        }
        RoutedCollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC);
        Bucket result = collect(collectNode);

        String expected = "id| string| NULL| false| true| 1| cluster| sys\n" +
                          "master_node| string| NULL| false| true| 2| cluster| sys\n" +
                          "name| string| NULL| false| true| 3| cluster| sys\n" +
                          "settings| object| NULL| false| true| 4| cluster| sys\n";


        assertThat(TestingHelpers.printedTable(result), Matchers.containsString(expected));

        // second time - to check if the internal iterator resets
        result = collect(collectNode);
        assertThat(TestingHelpers.printedTable(result), Matchers.containsString(expected));
    }
}
