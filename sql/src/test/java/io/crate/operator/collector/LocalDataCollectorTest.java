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
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertArrayEquals;

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
    private Routing testRouting = new Routing(new HashMap<String, Map<String, Integer>>(1){{
        put(TEST_NODE_ID, new HashMap<String, Integer>());
    }});
    private final static String TEST_NODE_ID = "test";

    class TestModule extends AbstractModule {
        protected MapBinder<FunctionIdent, FunctionImplementation> functionBinder;
        @Override
        protected void configure() {
            functionBinder = MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
            functionBinder.addBinding(TestFunction.ident).toInstance(new TestFunction());
            bind(Functions.class).asEagerSingleton();
        }
    }

    @Before
    public void configure() {
        Injector injector = new ModulesBuilder().add(
                new OperatorModule(),
                new TestModule()
        ).createInjector();

        functions = injector.getInstance(Functions.class);
        ReferenceResolver referenceResolver = new GlobalReferenceResolver(
                new HashMap<ReferenceIdent, ReferenceImplementation>(){{
                    put(TestExpression.ident, new TestExpression());
                }}
        );
        operation = new LocalDataCollectOperation(functions, referenceResolver);
    }

    @Test
    public void testCollectExpressions() {
        CollectNode collectNode = new CollectNode("collect", testRouting);
        Reference testReference = new Reference(TestExpression.info);

        collectNode.outputs(testReference);

        Object[][] result = operation.collect(TEST_NODE_ID, collectNode);

        assertThat(result.length, equalTo(1));

        assertThat((Integer) result[0][0], equalTo(42));
    }

    @Test
    public void testWrongRouting() {

        expectedException.expect(ElasticSearchIllegalStateException.class);
        expectedException.expectMessage("unsupported routing");

        CollectNode collectNode = new CollectNode("wrong", new Routing(new HashMap<String, Map<String, Integer>>(){{
            put("bla", new HashMap<String, Integer>(){{
                put("my_index", 1);
                put("my_index", 2);
            }});
        }}));
        operation.collect(TEST_NODE_ID, collectNode);
    }

    @Test
    public void testCollectNothing() {
        CollectNode collectNode = new CollectNode("nothing", testRouting);
        Object[][] result = operation.collect(TEST_NODE_ID, collectNode);
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
        Object[][] result = operation.collect(TEST_NODE_ID, collectNode);
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
        Object[][] result = operation.collect(TEST_NODE_ID, collectNode);
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
        operation.collect(TEST_NODE_ID, collectNode);
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
        Object[][] result = operation.collect(TEST_NODE_ID, collectNode);
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
        Object[][] result = operation.collect(TEST_NODE_ID, collectNode);
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
        Object[][] result = operation.collect(TEST_NODE_ID, collectNode);
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
        Object[][] result = operation.collect(TEST_NODE_ID, collectNode);
        assertArrayEquals(new Object[0][], result);
    }
}
