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

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.reference.sys.cluster.ClusterNameExpression;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.testing.TestingHelpers;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class HandlerSideLevelCollectTest extends SQLTransportIntegrationTest {

    private MapSideDataCollectOperation operation;
    private Functions functions;
    private RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

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
            whereClause.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );
    }

    private RoutedCollectPhase collectNode(Routing routing, List<Symbol> toCollect, RowGranularity rowGranularity) {
        return collectNode(routing, toCollect, rowGranularity, WhereClause.MATCH_ALL);
    }

    @Test
    public void testClusterLevel() throws Exception {
        Schemas schemas = internalCluster().getInstance(Schemas.class);
        TableInfo tableInfo = schemas.getTableInfo(new RelationName("sys", "cluster"));
        Routing routing = tableInfo.getRouting(
            clusterService().state(),
            routingProvider,
            WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, SessionContext.systemSessionContext());
        Reference clusterNameRef = new Reference(new ReferenceIdent(SysClusterTableInfo.IDENT, new ColumnIdent(ClusterNameExpression.NAME)), RowGranularity.CLUSTER, DataTypes.STRING);
        RoutedCollectPhase collectNode = collectNode(routing, Arrays.<Symbol>asList(clusterNameRef), RowGranularity.CLUSTER);
        Bucket result = collect(collectNode);
        assertThat(result.size(), is(1));
        assertThat(((String) result.iterator().next().get(0)), Matchers.startsWith("SUITE-"));
    }

    private Bucket collect(RoutedCollectPhase collectPhase) throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        BatchIterator<Row> bi = operation.createIterator(txnCtx, collectPhase, consumer.requiresScroll(), mock(CollectTask.class));
        consumer.accept(bi, null);
        return new CollectionBucket(consumer.getResult());
    }

    @Test
    public void testInformationSchemaTables() throws Exception {
        InformationSchemaInfo schemaInfo = internalCluster().getInstance(InformationSchemaInfo.class);
        TableInfo tablesTableInfo = schemaInfo.getTableInfo("tables");
        Routing routing = tablesTableInfo.getRouting(
            clusterService().state(),
            routingProvider,
            WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, SessionContext.systemSessionContext());
        List<Symbol> toCollect = new ArrayList<>();
        for (Reference reference : tablesTableInfo.columns()) {
            toCollect.add(reference);
        }
        Symbol tableNameRef = toCollect.get(12);

        List<Symbol> arguments = Arrays.asList(tableNameRef, Literal.of("shards"));
        FunctionImplementation eqImpl
            = functions.get(null, EqOperator.NAME, arguments, SearchPath.pathWithPGCatalogAndDoc());
        Function whereClause = new Function(eqImpl.info(), arguments);

        RoutedCollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC, new WhereClause(whereClause));
        Bucket result = collect(collectNode);
        assertThat(TestingHelpers.printedTable(result),
            is("NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| shards| sys| BASE TABLE| NULL\n"));
    }

    @Test
    public void testInformationSchemaColumns() throws Exception {
        InformationSchemaInfo schemaInfo = internalCluster().getInstance(InformationSchemaInfo.class);
        TableInfo tableInfo = schemaInfo.getTableInfo("columns");
        assert tableInfo != null;
        Routing routing = tableInfo.getRouting(
            clusterService().state(),
            routingProvider,
            WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, SessionContext.systemSessionContext());
        List<Symbol> toCollect = new ArrayList<>();
        for (Reference ref : tableInfo.columns()) {
            toCollect.add(ref);
        }
        RoutedCollectPhase collectNode = collectNode(routing, toCollect, RowGranularity.DOC);
        Bucket result = collect(collectNode);

        String expected =
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| id| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 1| sys| cluster| sys| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| license| object| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 2| sys| cluster| sys| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| license['expiry_date']| timestamp| 3| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| NULL| sys| cluster| sys| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| license['issued_to']| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| NULL| sys| cluster| sys| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| master_node| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 3| sys| cluster| sys| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 4| sys| cluster| sys| NULL| NULL| NULL";


        assertThat(TestingHelpers.printedTable(result), Matchers.containsString(expected));

        // second time - to check if the internal iterator resets
        result = collect(collectNode);
        assertThat(TestingHelpers.printedTable(result), Matchers.containsString(expected));
    }
}
