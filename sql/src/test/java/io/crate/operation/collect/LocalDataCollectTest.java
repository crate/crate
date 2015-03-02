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
import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.analyze.WhereClause;
import io.crate.blob.BlobEnvironment;
import io.crate.blob.v2.BlobIndices;
import io.crate.breaker.CircuitBreakerModule;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.*;
import io.crate.metadata.shard.ShardReferenceImplementation;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceImplementation;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.Input;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.reference.sys.shard.SysShardExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.InternalSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Answers;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalDataCollectTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static class TestExpression implements ReferenceImplementation, Input<Integer> {
        public static final ReferenceIdent ident = new ReferenceIdent(new TableIdent("default", "collect"), "truth");
        public static final ReferenceInfo info = new ReferenceInfo(ident, RowGranularity.NODE, DataTypes.INTEGER);

        @Override
        public Integer value() {
            return 42;
        }

        @Override
        public ReferenceImplementation getChildImplementation(String name) {
            return null;
        }
    }

    static class TestFunction extends Scalar<Integer, Object> {
        public static final FunctionIdent ident = new FunctionIdent("twoTimes", Arrays.<DataType>asList(DataTypes.INTEGER));
        public static final FunctionInfo info = new FunctionInfo(ident, DataTypes.INTEGER);

        @Override
        public Integer evaluate(Input[] args) {
            if (args.length == 0) {
                return 0;
            }
            Integer value = (Integer) args[0].value();
            return (value) * 2;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            return symbol;
        }
    }

    static class ShardIdExpression extends SysShardExpression<Integer> {

        private final ShardId shardId;

        @Inject
        public ShardIdExpression(ShardId shardId) {
            super("id");
            this.shardId = shardId;
        }

        @Override
        public Integer value() {
            return shardId.id();
        }

        @Override
        public ReferenceImplementation getChildImplementation(String name) {
            return null;
        }
    }

    private DiscoveryService discoveryService;
    private Functions functions;
    private IndexService indexService = mock(IndexService.class);
    private MapSideDataCollectOperation operation;
    private Routing testRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
        .put(TEST_NODE_ID, new TreeMap<String, List<Integer>>()).map()
    );


    private final ThreadPool testThreadPool = new ThreadPool(getClass().getSimpleName());
    private final static String TEST_NODE_ID = "test_node";
    private final static String TEST_TABLE_NAME = "test_table";

    private static Reference testNodeReference = new Reference(TestExpression.info);
    private static Reference testShardIdReference = new Reference(SysShardsTableInfo.INFOS.get(new ColumnIdent("id")));

    class TestModule extends AbstractModule {
        protected MapBinder<FunctionIdent, FunctionImplementation> functionBinder;

        @Override
        protected void configure() {
            functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
            functionBinder.addBinding(TestFunction.ident).toInstance(new TestFunction());
            bind(Functions.class).asEagerSingleton();
            bind(ThreadPool.class).toInstance(testThreadPool);

            bind(CircuitBreakerService.class).toInstance(new NoneCircuitBreakerService());
            bind(ActionFilters.class).toInstance(mock(ActionFilters.class));
            bind(ScriptService.class).toInstance(mock(ScriptService.class));
            bind(SearchService.class).toInstance(mock(InternalSearchService.class));
            bind(AllocationService.class).toInstance(mock(AllocationService.class));
            bind(DynamicSettings.class).annotatedWith(ClusterDynamicSettings.class).toInstance(mock(DynamicSettings.class));
            bind(MetaDataDeleteIndexService.class).toInstance(mock(MetaDataDeleteIndexService.class));
            bind(ClusterInfoService.class).toInstance(mock(ClusterInfoService.class));
            bind(TransportService.class).toInstance(mock(TransportService.class));

            bind(TransportShardBulkAction.class).toInstance(mock(TransportShardBulkAction.class));
            bind(TransportCreateIndexAction.class).toInstance(mock(TransportCreateIndexAction.class));
            bind(TransportQueryShardAction.class).toInstance(mock(TransportQueryShardAction.class));

            discoveryService = mock(DiscoveryService.class);
            DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
            when(discoveryNode.id()).thenReturn(TEST_NODE_ID);
            when(discoveryService.localNode()).thenReturn(discoveryNode);

            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.localNode()).thenReturn(discoveryNode);
            bind(ClusterService.class).toInstance(clusterService);

            IndicesService indicesService = mock(IndicesService.class);
            bind(IndicesService.class).toInstance(indicesService);
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            bind(MetaDataUpdateSettingsService.class).toInstance(mock(MetaDataUpdateSettingsService.class));
            bind(Client.class).toInstance(mock(Client.class));

            Provider<TransportCreateIndexAction> transportCreateIndexActionProvider = mock(Provider.class);
            when(transportCreateIndexActionProvider.get()).thenReturn(mock(TransportCreateIndexAction.class));
            Provider<TransportDeleteIndexAction> transportDeleteActionProvider = mock(Provider.class);
            when(transportDeleteActionProvider.get()).thenReturn(mock(TransportDeleteIndexAction.class));
            Provider<TransportUpdateSettingsAction> transportUpdateSettingsActionProvider = mock(Provider.class);
            when(transportUpdateSettingsActionProvider.get()).thenReturn(mock(TransportUpdateSettingsAction.class));

            BlobIndices blobIndices = new BlobIndices(
                    ImmutableSettings.EMPTY,
                    transportCreateIndexActionProvider,
                    transportDeleteActionProvider,
                    transportUpdateSettingsActionProvider,
                    indicesService,
                    mock(IndicesLifecycle.class),
                    mock(BlobEnvironment.class),
                    clusterService
            );
            bind(BlobIndices.class).toInstance(blobIndices);

            bind(ReferenceResolver.class).to(GlobalReferenceResolver.class);
            MapBinder<ReferenceIdent, ReferenceImplementation> binder = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
            binder.addBinding(TestExpression.ident).toInstance(new TestExpression());

            TransportPutIndexTemplateAction transportPutIndexTemplateAction = mock(TransportPutIndexTemplateAction.class);
            bind(TransportPutIndexTemplateAction.class).toInstance(transportPutIndexTemplateAction);

            bind(IndexService.class).toInstance(indexService);
        }
    }

    class TestShardModule extends AbstractModule {

        private final ShardId shardId;
        private final ShardIdExpression shardIdExpression;

        public TestShardModule(int shardId) {
            super();
            this.shardId = new ShardId(TEST_TABLE_NAME, shardId);
            this.shardIdExpression = new ShardIdExpression(this.shardId);
        }

        @Override
        protected void configure() {
            IndexShard shard = mock(InternalIndexShard.class);
            bind(IndexShard.class).toInstance(shard);
            Index index = new Index(TEST_TABLE_NAME);
            bind(Index.class).toInstance(index);
            bind(ShardId.class).toInstance(shardId);
            MapBinder<ReferenceIdent, ShardReferenceImplementation> binder = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, ShardReferenceImplementation.class);
            binder.addBinding(SysShardsTableInfo.INFOS.get(new ColumnIdent("id")).ident()).toInstance(shardIdExpression);
            bind(ShardReferenceResolver.class).asEagerSingleton();
            bind(AllocationDecider.class).to(DiskThresholdDecider.class);
            bind(ShardCollectService.class).asEagerSingleton();

            bind(DiscoveryService.class).toInstance(discoveryService);

            // blob stuff
            MapBinder<ReferenceIdent, BlobShardReferenceImplementation> blobBinder = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, BlobShardReferenceImplementation.class);
            bind(Settings.class).annotatedWith(IndexSettings.class).toInstance(ImmutableSettings.EMPTY);
        }
    }

    @Before
    public void configure() {
        Injector injector = new ModulesBuilder().add(
                new CircuitBreakerModule(),
                new OperatorModule(),
                new TestModule()
        ).createInjector();
        Injector shard0Injector = injector.createChildInjector(
                new TestShardModule(0)
        );
        Injector shard1Injector = injector.createChildInjector(
                new TestShardModule(1)
        );
        functions = injector.getInstance(Functions.class);

        IndicesService indicesService = injector.getInstance(IndicesService.class);
        indexService = injector.getInstance(IndexService.class);

        when(indexService.shardInjectorSafe(0)).thenReturn(shard0Injector);
        when(indexService.shardInjectorSafe(1)).thenReturn(shard1Injector);
        when(indexService.shardSafe(0)).thenReturn(shard0Injector.getInstance(IndexShard.class));
        when(indexService.shardSafe(1)).thenReturn(shard1Injector.getInstance(IndexShard.class));
        when(indicesService.indexServiceSafe(TEST_TABLE_NAME)).thenReturn(indexService);

        NodeSettingsService nodeSettingsService = mock(NodeSettingsService.class);

        operation = new MapSideDataCollectOperation(
                injector.getInstance(ClusterService.class),
                ImmutableSettings.EMPTY,
                mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
                functions, injector.getInstance(ReferenceResolver.class), indicesService, testThreadPool,
                new CollectServiceResolver(discoveryService,
                    new SystemCollectService(
                            discoveryService,
                            functions,
                            new StatsTables(ImmutableSettings.EMPTY, nodeSettingsService))
                ));
    }

    private Routing shardRouting(final Integer... shardIds) {
        return new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put(TEST_NODE_ID, TreeMapBuilder.<String, List<Integer>>newMapBuilder()
                            .put(TEST_TABLE_NAME, Arrays.asList(shardIds))
                            .map()
            )
            .map()
        );
    }

    @Test
    public void testCollectExpressions() throws Exception {
        CollectNode collectNode = new CollectNode("collect", testRouting);
        collectNode.maxRowGranularity(RowGranularity.NODE);
        collectNode.toCollect(Arrays.<Symbol>asList(testNodeReference));
        collectNode.jobId(UUID.randomUUID());
        Object[][] result = operation.collect(collectNode, null).get();

        assertThat(result.length, equalTo(1));

        assertThat((Integer) result[0][0], equalTo(42));
    }

    @Test
    public void testWrongRouting() throws Exception {

        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("unsupported routing");

        CollectNode collectNode = new CollectNode("wrong", new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("bla", TreeMapBuilder.<String, List<Integer>>newMapBuilder()
                .put("my_index", Arrays.asList(1))
                .put("my_index", Arrays.asList(1))
                .map()
            ).map()
        ));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.jobId(UUID.randomUUID());
        operation.collect(collectNode, null);
    }

    @Test
    public void testCollectUnknownReference() throws Throwable {
        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("Unknown Reference some.table.some_column");

        CollectNode collectNode = new CollectNode("unknown", testRouting);
        Reference unknownReference = new Reference(
                new ReferenceInfo(
                        new ReferenceIdent(
                                new TableIdent("some", "table"),
                                "some_column"
                        ),
                        RowGranularity.NODE,
                        DataTypes.BOOLEAN
                )
        );
        collectNode.toCollect(Arrays.<Symbol>asList(unknownReference));
        collectNode.maxRowGranularity(RowGranularity.NODE);
        collectNode.jobId(UUID.randomUUID());
        try {
            operation.collect(collectNode, null).get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testCollectFunction() throws Exception {
        CollectNode collectNode = new CollectNode("function", testRouting);
        Function twoTimesTruthFunction = new Function(
                TestFunction.info,
                Arrays.<Symbol>asList(testNodeReference)
        );
        collectNode.toCollect(Arrays.<Symbol>asList(twoTimesTruthFunction, testNodeReference));
        collectNode.maxRowGranularity(RowGranularity.NODE);
        collectNode.jobId(UUID.randomUUID());
        Object[][] result = operation.collect(collectNode, null).get();
        assertThat(result.length, equalTo(1));
        assertThat(result[0].length, equalTo(2));
        assertThat((Integer) result[0][0], equalTo(84));
        assertThat((Integer) result[0][1], equalTo(42));
    }


    @Test
    public void testUnknownFunction() throws Throwable {
        // will be wrapped somewhere above
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot find implementation for function unknown()");

        CollectNode collectNode = new CollectNode("unknownFunction", testRouting);
        Function unknownFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent("unknown", ImmutableList.<DataType>of()),
                        DataTypes.BOOLEAN
                ),
                ImmutableList.<Symbol>of()
        );
        collectNode.toCollect(Arrays.<Symbol>asList(unknownFunction));
        collectNode.jobId(UUID.randomUUID());
        try {
            operation.collect(collectNode, null).get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testCollectLiterals() throws Exception {
        CollectNode collectNode = new CollectNode("literals", testRouting);
        collectNode.toCollect(Arrays.<Symbol>asList(
                Literal.newLiteral("foobar"),
                Literal.newLiteral(true),
                Literal.newLiteral(1),
                Literal.newLiteral(4.2)
        ));
        collectNode.jobId(UUID.randomUUID());
        Object[][] result = operation.collect(collectNode, null).get();
        assertThat(result.length, equalTo(1));
        assertThat((BytesRef) result[0][0], equalTo(new BytesRef("foobar")));
        assertThat((Boolean) result[0][1], equalTo(true));
        assertThat((Integer) result[0][2], equalTo(1));
        assertThat((Double) result[0][3], equalTo(4.2));

    }

    @Test
    public void testCollectWithFalseWhereClause() throws Exception {
        CollectNode collectNode = new CollectNode("whereClause", testRouting);
        collectNode.toCollect(Arrays.<Symbol>asList(testNodeReference));
        collectNode.whereClause(new WhereClause(new Function(
                AndOperator.INFO,
                Arrays.<Symbol>asList(Literal.newLiteral(false), Literal.newLiteral(false))
        )));
        collectNode.jobId(UUID.randomUUID());
        Object[][] result = operation.collect(collectNode, null).get();
        assertArrayEquals(new Object[0][], result);
    }

    @Test
    public void testCollectWithTrueWhereClause() throws Exception {
        CollectNode collectNode = new CollectNode("whereClause", testRouting);
        collectNode.toCollect(Arrays.<Symbol>asList(testNodeReference));
        collectNode.whereClause(new WhereClause(new Function(
                AndOperator.INFO,
                Arrays.<Symbol>asList(Literal.newLiteral(true), Literal.newLiteral(true))
        )));
        collectNode.jobId(UUID.randomUUID());
        collectNode.maxRowGranularity(RowGranularity.NODE);
        Object[][] result = operation.collect(collectNode, null).get();
        assertThat(result.length, equalTo(1));
        assertThat((Integer) result[0][0], equalTo(42));

    }

    @Test
    public void testCollectWithNullWhereClause() throws Exception {
        EqOperator op = (EqOperator) functions.get(new FunctionIdent(
                EqOperator.NAME, ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.INTEGER)));
        CollectNode collectNode = new CollectNode("whereClause", testRouting);
        collectNode.toCollect(Arrays.<Symbol>asList(testNodeReference));
        collectNode.whereClause(new WhereClause(new Function(
                op.info(),
                Arrays.<Symbol>asList(Literal.NULL, Literal.NULL)
        )));
        collectNode.jobId(UUID.randomUUID());
        Object[][] result = operation.collect(collectNode, null).get();
        assertArrayEquals(new Object[0][], result);
    }

    @Test
    public void testCollectShardExpressions() throws Exception {
        CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0, 1));
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference));
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        collectNode.jobId(UUID.randomUUID());
        Object[][] result = operation.collect(collectNode, null).get();
        assertThat(result.length, is(equalTo(2)));
        assertThat((Integer) result[0][0], isOneOf(0, 1));
        assertThat((Integer) result[1][0], isOneOf(0, 1));

    }

    @Test
    public void testCollectShardExpressionsWhereShardIdIs0() throws Exception {
        EqOperator op = (EqOperator) functions.get(new FunctionIdent(
                EqOperator.NAME, ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.INTEGER)));

        CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0, 1));
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference));
        collectNode.whereClause(new WhereClause(
                new Function(op.info(), Arrays.<Symbol>asList(testShardIdReference, Literal.newLiteral(0)))));
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        collectNode.jobId(UUID.randomUUID());
        Object[][] result = operation.collect(collectNode, null).get();
        assertThat(result.length, is(equalTo(1)));
        assertThat((Integer) result[0][0], is(0));
    }

    @Test
    public void testCollectShardExpressionsLiteralsAndNodeExpressions() throws Exception {
        CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0, 1));
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference, Literal.newLiteral(true), testNodeReference));
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        collectNode.jobId(UUID.randomUUID());
        Object[][] result = operation.collect(collectNode, null).get();
        assertThat(result.length, is(equalTo(2)));
        assertThat(result[0].length, is(equalTo(3)));
        int i, j;
        if (result[0][0] == 0) {
            i = 0;
            j = 1;
        } else {
            i = 1;
            j = 0;
        }
        assertThat((Integer) result[i][0], is(0));
        assertThat((Boolean) result[i][1], is(true));
        assertThat((Integer) result[i][2], is(42));

        assertThat((Integer) result[j][0], is(1));
        assertThat((Boolean) result[j][1], is(true));
        assertThat((Integer) result[j][2], is(42));
    }
}
