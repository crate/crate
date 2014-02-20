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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.metadata.shard.ShardReferenceImplementation;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operator.Input;
import io.crate.operator.operator.AndOperator;
import io.crate.operator.operator.EqOperator;
import io.crate.operator.operator.OperatorModule;
import io.crate.operator.reference.sys.shard.SysShardExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.CollectNode;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.SQLXContentQueryParser;
import org.cratedb.sql.CrateException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalDataCollectTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static class TestExpression implements ReferenceImplementation, Input<Integer> {
        public static final ReferenceIdent ident = new ReferenceIdent(new TableIdent("default", "collect"), "truth");
        public static final ReferenceInfo info = new ReferenceInfo(ident, RowGranularity.NODE, DataType.INTEGER);

        @Override
        public Integer value() {
            return 42;
        }

        @Override
        public ReferenceInfo info() {
            return info;
        }

        @Override
        public ReferenceImplementation getChildImplementation(String name) {
            return null;
        }
    }

    static class TestFunction implements Scalar<Integer, Object> {
        public static final FunctionIdent ident = new FunctionIdent("twoTimes", Arrays.asList(DataType.INTEGER));
        public static final FunctionInfo info = new FunctionInfo(ident, DataType.INTEGER, false);

        @Override
        public Integer evaluate(Input<Object>... args) {
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

    private Functions functions;
    private IndexService indexService = mock(IndexService.class);
    private LocalDataCollectOperation operation;
    private Routing testRouting = new Routing(new HashMap<String, Map<String, Set<Integer>>>(1) {{
        put(TEST_NODE_ID, new HashMap<String, Set<Integer>>());
    }});


    private final ThreadPool testThreadPool = new ThreadPool();
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

            bind(SQLXContentQueryParser.class).toInstance(mock(SQLXContentQueryParser.class));

            DiscoveryNode mockedNode = mock(DiscoveryNode.class);
            when(mockedNode.id()).thenReturn(TEST_NODE_ID);
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.localNode()).thenReturn(mockedNode);
            bind(ClusterService.class).toInstance(clusterService);

            IndicesService indicesService = mock(IndicesService.class);
            bind(IndicesService.class).toInstance(indicesService);
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            bind(ReferenceResolver.class).to(GlobalReferenceResolver.class);
            MapBinder<ReferenceIdent, ReferenceImplementation> binder = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
            binder.addBinding(TestExpression.ident).toInstance(new TestExpression());

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
            bind(ShardId.class).toInstance(shardId);
            MapBinder<ReferenceIdent, ShardReferenceImplementation> binder = MapBinder
                    .newMapBinder(binder(), ReferenceIdent.class, ShardReferenceImplementation.class);
            binder.addBinding(SysShardsTableInfo.INFOS.get(new ColumnIdent("id")).ident()).toInstance(shardIdExpression);
            bind(ShardReferenceResolver.class).asEagerSingleton();
            bind(ScriptService.class).toInstance(mock(ScriptService.class));
            bind(ShardCollectService.class).asEagerSingleton();
        }
    }

    @Before
    public void configure() {
        Injector injector = new ModulesBuilder().add(
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

        operation = new LocalDataCollectOperation(injector.getInstance(ClusterService.class),
                functions, injector.getInstance(ReferenceResolver.class), indicesService, testThreadPool);
    }

    private Routing shardRouting(final Integer... shardIds) {
        return new Routing(new HashMap<String, Map<String, Set<Integer>>>() {{
            put(TEST_NODE_ID, new HashMap<String, Set<Integer>>() {{
                put(TEST_TABLE_NAME, ImmutableSet.copyOf(shardIds));
            }});
        }});
    }

    @Test
    public void testCollectExpressions() throws Exception {
        CollectNode collectNode = new CollectNode("collect", testRouting);
        collectNode.maxRowGranularity(RowGranularity.NODE);
        collectNode.toCollect(Arrays.<Symbol>asList(testNodeReference));

        Object[][] result = operation.collect(collectNode).get();

        assertThat(result.length, equalTo(1));

        assertThat((Integer) result[0][0], equalTo(42));
    }

    @Test
    public void testWrongRouting() throws Exception {

        expectedException.expect(CrateException.class);
        expectedException.expectMessage("unsupported routing");

        CollectNode collectNode = new CollectNode("wrong", new Routing(new HashMap<String, Map<String, Set<Integer>>>() {{
            put("bla", new HashMap<String, Set<Integer>>() {{
                put("my_index", Sets.newHashSet(1));
                put("my_index", Sets.newHashSet(1));
            }});
        }}));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        operation.collect(collectNode);
    }

    @Test
    public void testCollectUnknownReference() throws Exception {

        expectedException.expect(CrateException.class);
        expectedException.expectMessage("Unknown Reference");

        CollectNode collectNode = new CollectNode("unknown", testRouting);
        Reference unknownReference = new Reference(
                new ReferenceInfo(
                        new ReferenceIdent(
                                new TableIdent("", ""),
                                ""
                        ),
                        RowGranularity.NODE,
                        DataType.BOOLEAN
                )
        );
        collectNode.toCollect(Arrays.<Symbol>asList(unknownReference));
        collectNode.maxRowGranularity(RowGranularity.NODE);
        operation.collect(collectNode);
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
        Object[][] result = operation.collect(collectNode).get();
        assertThat(result.length, equalTo(1));
        assertThat(result[0].length, equalTo(2));
        assertThat((Integer) result[0][0], equalTo(84));
        assertThat((Integer) result[0][1], equalTo(42));
    }


    @Test
    public void testUnknownFunction() throws Exception {
        // will be wrapped somewhere above
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot find implementation for function unknown()");

        CollectNode collectNode = new CollectNode("unknownFunction", testRouting);
        Function unknownFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent("unknown", ImmutableList.<DataType>of()),
                        DataType.BOOLEAN,
                        false
                ),
                ImmutableList.<Symbol>of()
        );
        collectNode.toCollect(Arrays.<Symbol>asList(unknownFunction));
        operation.collect(collectNode);
    }

    @Test
    public void testCollectLiterals() throws Exception {
        CollectNode collectNode = new CollectNode("literals", testRouting);
        collectNode.toCollect(Arrays.<Symbol>asList(
                new StringLiteral("foobar"),
                new BooleanLiteral(true),
                new IntegerLiteral(1),
                new DoubleLiteral(4.2)
        ));
        Object[][] result = operation.collect(collectNode).get();
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
                Arrays.<Symbol>asList(new BooleanLiteral(false), new BooleanLiteral(false))
        )));
        Object[][] result = operation.collect(collectNode).get();
        assertArrayEquals(new Object[0][], result);
    }

    @Test
    public void testCollectWithTrueWhereClause() throws Exception {
        CollectNode collectNode = new CollectNode("whereClause", testRouting);
        collectNode.toCollect(Arrays.<Symbol>asList(testNodeReference));
        collectNode.whereClause(new WhereClause(new Function(
                AndOperator.INFO,
                Arrays.<Symbol>asList(new BooleanLiteral(true), new BooleanLiteral(true))
        )));
        collectNode.maxRowGranularity(RowGranularity.NODE);
        Object[][] result = operation.collect(collectNode).get();
        assertThat(result.length, equalTo(1));
        assertThat((Integer) result[0][0], equalTo(42));

    }

    @Test
    public void testCollectWithNullWhereClause() throws Exception {
        EqOperator op = (EqOperator) functions.get(new FunctionIdent(EqOperator.NAME, ImmutableList.of(DataType.INTEGER, DataType.INTEGER)));
        CollectNode collectNode = new CollectNode("whereClause", testRouting);
        collectNode.toCollect(Arrays.<Symbol>asList(testNodeReference));
        collectNode.whereClause(new WhereClause(new Function(
                op.info(),
                Arrays.<Symbol>asList(Null.INSTANCE, Null.INSTANCE)
        )));
        Object[][] result = operation.collect(collectNode).get();
        assertArrayEquals(new Object[0][], result);
    }

    @Test
    public void testCollectShardExpressions() throws Exception {
        CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0, 1));
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference));
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        Object[][] result = operation.collect(collectNode).get();
        assertThat(result.length, is(equalTo(2)));
        assertThat((Integer) result[0][0], isOneOf(0, 1));
        assertThat((Integer) result[1][0], isOneOf(0, 1));

    }

    @Test
    public void testCollectShardExpressionsWhereShardIdIs0() throws Exception {
        EqOperator op = (EqOperator) functions.get(new FunctionIdent(
                EqOperator.NAME, ImmutableList.of(DataType.INTEGER, DataType.INTEGER)));

        CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0, 1));
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference));
        collectNode.whereClause(new WhereClause(
                new Function(op.info(), Arrays.<Symbol>asList(testShardIdReference, new IntegerLiteral(0)))));
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        Object[][] result = operation.collect(collectNode).get();
        assertThat(result.length, is(equalTo(1)));
        assertThat((Integer) result[0][0], is(0));
    }

    @Test
    public void testCollectShardExpressionsLiteralsAndNodeExpressions() throws Exception {
        CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0, 1));
        collectNode.toCollect(Arrays.<Symbol>asList(testShardIdReference, new BooleanLiteral(true), testNodeReference));
        collectNode.maxRowGranularity(RowGranularity.SHARD);
        Object[][] result = operation.collect(collectNode).get();
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

    /**
     @Test public void testCollectShardExpressionsLimit1() throws Exception {
     CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0,1), null, 1, CollectNode.NO_OFFSET);
     collectNode.toCollect(testShardIdReference, testNodeReference);
     Object result[][] = operation.collect(collectNode).get();
     assertThat(result.length, is(1));
     assertThat(result[0].length, is(2));
     assertThat((Integer)result[0][0], isOneOf(0,1));
     assertThat((Integer)result[0][1], is(42));
     }

     @Test public void testCollectShardExpressionsNoLimitOffset2() throws Exception {
     CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0,1), null, CollectNode.NO_LIMIT, 2);
     collectNode.toCollect(testShardIdReference, testNodeReference);
     Object result[][] = operation.collect(collectNode).get();
     assertThat(result.length, is(0));
     }

     @Test public void testCollectShardExpressionsLimit0() throws Exception {
     CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0,1), null, 0, CollectNode.NO_OFFSET);
     collectNode.toCollect(testShardIdReference, testNodeReference);
     Object result[][] = operation.collect(collectNode).get();
     assertThat(result.length, is(0));
     }

     @Test public void testCollectNodeExpressionsLimit0() throws Exception {
     CollectNode collectNode = new CollectNode("nodeCollect", testRouting, null, 0, CollectNode.NO_OFFSET);
     collectNode.toCollect(testNodeReference);
     Object result[][] = operation.collect(collectNode).get();
     assertThat(result.length, is(0));
     }

     @Test public void testCollectNodeExpressionsOffset1() throws Exception {
     CollectNode collectNode = new CollectNode("nodeCollect", testRouting, null, CollectNode.NO_LIMIT, 1);
     collectNode.toCollect(testNodeReference);
     Object result[][] = operation.collect(collectNode).get();
     assertThat(result.length, is(0));
     }

     @Test public void testCollectShardExpressionsOrderByAsc() throws Exception {
     CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0,1), null, CollectNode.NO_LIMIT, CollectNode.NO_OFFSET, new int[]{0}, new boolean[]{false});
     collectNode.toCollect(testShardIdReference, testNodeReference);
     Object result[][] = operation.collect(collectNode).get();
     assertThat(result.length, is(2));
     assertThat((Integer)result[0][0], is(0));
     assertThat((Integer)result[1][0], is(1));
     }

     @Test public void testCollectShardExpressionsOrderByDesc() throws Exception {
     CollectNode collectNode = new CollectNode("shardCollect", shardRouting(0,1), null, CollectNode.NO_LIMIT, CollectNode.NO_OFFSET, new int[]{0}, new boolean[]{true});
     collectNode.toCollect(testShardIdReference, testNodeReference);
     Object result[][] = operation.collect(collectNode).get();
     assertThat(result.length, is(2));
     assertThat((Integer)result[0][0], is(1));
     assertThat((Integer)result[1][0], is(0));
     }
     */
}
