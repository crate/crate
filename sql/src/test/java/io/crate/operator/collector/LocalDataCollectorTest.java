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
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.operations.collect.LocalDataCollectOperation;
import io.crate.planner.RowGranularity;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.ValueSymbol;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

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
        public static final FunctionIdent ident = new FunctionIdent("twoTimes", ImmutableList.of(DataType.INTEGER));
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
    }

    private LocalDataCollectOperation operation;
    private Routing testRouting = new Routing(new HashMap<String, Map<String, Integer>>(1){{
        put(TEST_NODE_ID, new HashMap<String, Integer>());
    }});
    private final static String TEST_NODE_ID = "test";

    @Before
    public void configure() {
        Functions functions = new Functions(
                new HashMap<FunctionIdent, FunctionImplementation>(){{
                    put(TestFunction.ident, new TestFunction());
                }}
        );
        ReferenceResolver referenceResolver = new GlobalReferenceResolver(
                new HashMap<ReferenceIdent, ReferenceImplementation>(){{
                    put(TestExpression.ident, new TestExpression());
                }}
        );
        ImplementationSymbolVisitor symbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions
        );
        operation = new LocalDataCollectOperation(symbolVisitor);
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
                new ArrayList<ValueSymbol>(){{
                    add(truthReference);
                }}
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
                ImmutableList.<ValueSymbol>of()
        );
        collectNode.outputs(unknownFunction);
        operation.collect(TEST_NODE_ID, collectNode);
    }
}
