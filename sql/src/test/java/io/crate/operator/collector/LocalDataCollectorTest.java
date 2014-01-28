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

package io.crate.operator.collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.crate.metadata.*;
import io.crate.operator.Input;
import io.crate.operator.operations.collect.LocalDataCollectOperation;
import io.crate.operator.operator.AndOperator;
import io.crate.operator.operator.EqOperator;
import io.crate.operator.operator.OperatorModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalDataCollectorTest {

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

    static class TestFunction implements Scalar<Integer> {
        public static final FunctionIdent ident = new FunctionIdent("twoTimes", Arrays.asList(DataType.INTEGER));
        public static final FunctionInfo info = new FunctionInfo(ident, DataType.INTEGER, false);

        @Override
        public Integer evaluate(Input<?>... args) {
            if (args.length == 0) { return 0; }
            Integer value = (Integer)args[0].value();
            return (value)*2;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public Symbol normalizeSymbol(Symbol symbol) {
            return symbol;
        }
    }

    private Functions functions;
    private LocalDataCollectOperation operation;
    private Routing testRouting = new Routing(new HashMap<String, Map<String, Set<Integer>>>(1){{
        put(TEST_NODE_ID, new HashMap<String, Set<Integer>>());
    }});
    private final ThreadPool testThreadPool = new ThreadPool();
    private final static String TEST_NODE_ID = "test_node";
    private final static String TEST_TABLE_NAME = "test_table";

    class TestModule extends AbstractModule {
        protected MapBinder<FunctionIdent, FunctionImplementation> functionBinder;
        @Override
        protected void configure() {
            functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
            functionBinder.addBinding(TestFunction.ident).toInstance(new TestFunction());
            bind(Functions.class).asEagerSingleton();
            bind(ThreadPool.class).toInstance(testThreadPool);
        }
    }

    static class ShardIdExpression implements ReferenceImplementation, Input<Integer> {
        public static final ReferenceIdent ident = new ReferenceIdent(new TableIdent("sys", "shards"), "id");
        public static final ReferenceInfo info = new ReferenceInfo(ident, RowGranularity.SHARD, DataType.INTEGER);

        private final ShardId shardId;

        @Inject
        public ShardIdExpression(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public Integer value() {
            return shardId.id();
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
            bind(ShardId.class).toInstance(shardId);
            bind(ReferenceResolver.class).toInstance(new GlobalReferenceResolver(new HashMap<ReferenceIdent, ReferenceImplementation>(){{
               put(ShardIdExpression.ident, shardIdExpression);
            }}));
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
        ReferenceResolver referenceResolver = new GlobalReferenceResolver(
                new HashMap<ReferenceIdent, ReferenceImplementation>(){{
                    put(TestExpression.ident, new TestExpression());
                }}
        );

        IndicesService indicesService = mock(IndicesService.class);
        IndexService testIndexService = mock(IndexService.class);

        when(testIndexService.shardInjectorSafe(0)).thenReturn(shard0Injector);
        when(testIndexService.shardInjectorSafe(1)).thenReturn(shard1Injector);
        when(indicesService.indexServiceSafe(TEST_TABLE_NAME)).thenReturn(testIndexService);

        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode mockedNode = mock(DiscoveryNode.class);
        when(mockedNode.id()).thenReturn(TEST_NODE_ID);
        when(clusterService.localNode()).thenReturn(mockedNode);
        operation = new LocalDataCollectOperation(clusterService, functions, referenceResolver, indicesService, testThreadPool);
    }

    @Test
    public void testCollectExpressions() {
        CollectNode collectNode = new CollectNode("collect", testRouting);
        Reference testReference = new Reference(TestExpression.info);

        collectNode.outputs(testReference);

        Object[][] result = operation.collect(collectNode);

        assertThat(result.length, equalTo(1));

        assertThat((Integer) result[0][0], equalTo(42));
    }

    @Test
    public void testWrongRouting() {

        expectedException.expect(ElasticSearchIllegalStateException.class);
        expectedException.expectMessage("unsupported routing");

        CollectNode collectNode = new CollectNode("wrong", new Routing(new HashMap<String, Map<String, Set<Integer>>>(){{
            put("bla", new HashMap<String, Set<Integer>>(){{
                put("my_index", Sets.newHashSet(1));
                put("my_index", Sets.newHashSet(1));
            }});
        }}));
        operation.collect(collectNode);
    }

    @Test
    public void testCollectNothing() {
        CollectNode collectNode = new CollectNode("nothing", testRouting);
        Object[][] result = operation.collect(collectNode);
        assertThat(result, equalTo(new Object[0]));
    }

    @Test
    public void testCollectUnknownReference() {

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
        collectNode.outputs(unknownReference);
        Object[][] result = operation.collect(collectNode);
    }

    @Test
    public void testCollectFunction() {
        CollectNode collectNode = new CollectNode("function", testRouting);
        final Reference truthReference = new Reference(TestExpression.info);
        Function twoTimesTruthFunction = new Function(
                TestFunction.info,
                Arrays.<Symbol>asList(truthReference)
        );
        collectNode.outputs(twoTimesTruthFunction, truthReference);
        Object[][] result = operation.collect(collectNode);
        assertThat(result.length, equalTo(1));
        assertThat(result[0].length, equalTo(2));
        assertThat((Integer)result[0][0], equalTo(84));
        assertThat((Integer)result[0][1], equalTo(42));
    }


    @Test
    public void testUnknownFunction() {

        expectedException.expect(CrateException.class);
        expectedException.expectMessage("Unknown Function");

        CollectNode collectNode = new CollectNode("unknownFunction", testRouting);
        Function unknownFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent("", ImmutableList.<DataType>of()),
                        DataType.BOOLEAN,
                        false
                ),
                ImmutableList.<Symbol>of()
        );
        collectNode.outputs(unknownFunction);
        operation.collect(collectNode);
    }

    @Test
    public void testCollectLiterals() {
        CollectNode collectNode = new CollectNode("literals", testRouting);
        collectNode.outputs(
                new StringLiteral("foobar"),
                new BooleanLiteral(true),
                new IntegerLiteral(1),
                new DoubleLiteral(4.2)
        );
        Object[][] result = operation.collect(collectNode);
        assertThat(result.length, equalTo(1));
        assertThat((String) result[0][0], equalTo("foobar"));
        assertThat((Boolean) result[0][1], equalTo(true));
        assertThat((Integer)result[0][2], equalTo(1));
        assertThat((Double)result[0][3], equalTo(4.2));

    }

    @Test
    public void testCollectWithFalseWhereClause() {
        CollectNode collectNode = new CollectNode("whereClause", testRouting,
                new Function(
                        AndOperator.INFO,
                        Arrays.<Symbol>asList(new BooleanLiteral(false), new BooleanLiteral(false))
                )
        );
        Reference testReference = new Reference(TestExpression.info);
        collectNode.outputs(testReference);
        Object[][] result = operation.collect(collectNode);
        assertArrayEquals(new Object[0][], result);
    }

    @Test
    public void testCollectWithTrueWhereClause() {
        CollectNode collectNode = new CollectNode("whereClause", testRouting,
                new Function(
                        AndOperator.INFO,
                        Arrays.<Symbol>asList(new BooleanLiteral(true), new BooleanLiteral(true))
                )
        );
        Reference testReference = new Reference(TestExpression.info);
        collectNode.outputs(testReference);
        Object[][] result = operation.collect(collectNode);
        assertThat(result.length, equalTo(1));
        assertThat((Integer)result[0][0], equalTo(42));

    }

    @Test
    public void testCollectWithNullWhereClause() {
        EqOperator op = (EqOperator)functions.get(new FunctionIdent(EqOperator.NAME, ImmutableList.of(DataType.INTEGER, DataType.INTEGER)));
        CollectNode collectNode = new CollectNode("whereClause", testRouting,
                new Function(
                        op.info(),
                        Arrays.<Symbol>asList(Null.INSTANCE, Null.INSTANCE)
                )
        );
        Reference testReference = new Reference(TestExpression.info);
        collectNode.outputs(testReference);
        Object[][] result = operation.collect(collectNode);
        assertArrayEquals(new Object[0][], result);
    }

    @Test
    public void testCollectShardExpressions() {
        Routing shardRouting = new Routing(new HashMap<String, Map<String, Set<Integer>>>(){{
            put(TEST_NODE_ID, new HashMap<String, Set<Integer>>(){{
                put(TEST_TABLE_NAME, ImmutableSet.<Integer>of(0, 1));
            }});
        }});
        CollectNode collectNode = new CollectNode("shardCollect", shardRouting);
        Reference testReference = new Reference(ShardIdExpression.info);
        collectNode.outputs(testReference);
        Object[][] result = operation.collect(collectNode);
        assertThat(result.length, is(equalTo(2)));
        assertThat((Integer)result[0][0], Matchers.isOneOf(0, 1));
        assertThat((Integer)result[1][0], Matchers.isOneOf(0, 1));
    }

    @Test
    public void testCollectShardExpressionsWhereShardIdIs0() {
        Routing shardRouting = new Routing(new HashMap<String, Map<String, Set<Integer>>>(){{
            put(TEST_NODE_ID, new HashMap<String, Set<Integer>>(){{
                put(TEST_TABLE_NAME, ImmutableSet.<Integer>of(0, 1));
            }});
        }});
        EqOperator op = (EqOperator)functions.get(new FunctionIdent(EqOperator.NAME, ImmutableList.of(DataType.INTEGER, DataType.INTEGER)));

        CollectNode collectNode = new CollectNode("shardCollect", shardRouting,
                new Function(op.info(), Arrays.<Symbol>asList(new Reference(ShardIdExpression.info), new IntegerLiteral(0))));
        Reference testReference = new Reference(ShardIdExpression.info);
        collectNode.outputs(testReference);
        Object[][] result = operation.collect(collectNode);
        assertThat(result.length, is(equalTo(1)));
        assertThat((Integer)result[0][0], is(0));
    }

    @Test
    public void testCollectShardExpressionsLiteralsAndNodeExpressions() {
        Routing shardRouting = new Routing(new HashMap<String, Map<String, Set<Integer>>>(){{
            put(TEST_NODE_ID, new HashMap<String, Set<Integer>>(){{
                put(TEST_TABLE_NAME, ImmutableSet.<Integer>of(0, 1));
            }});
        }});
        CollectNode collectNode = new CollectNode("shardCollect", shardRouting);
        Reference testShardReference = new Reference(ShardIdExpression.info);
        Reference testNodeReference = new Reference(TestExpression.info);
        collectNode.outputs(testShardReference, new BooleanLiteral(true), testNodeReference);
        Object[][] result = operation.collect(collectNode);
        assertThat(result.length, is(equalTo(2)));
        assertThat(result[0].length, is(equalTo(3)));
        assertThat((Integer)result[0][0], is(0));
        assertThat((Boolean)result[0][1], is(true));
        assertThat((Integer)result[0][2], is(42));

        assertThat((Integer)result[1][0], is(1));
        assertThat((Boolean)result[1][1], is(true));
        assertThat((Integer)result[1][2], is(42));

    }
}
