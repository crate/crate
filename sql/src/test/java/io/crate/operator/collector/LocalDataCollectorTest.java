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

import io.crate.metadata.*;
import io.crate.operator.Input;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.operations.collect.LocalDataCollectOperation;
import io.crate.planner.RowGranularity;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Reference;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class LocalDataCollectorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static class TestExpression implements ReferenceImplementation, Input<String> {
        public static final ReferenceIdent ident = new ReferenceIdent(new TableIdent("default", "collect"), "test");
        public static final ReferenceInfo info = new ReferenceInfo(ident, RowGranularity.NODE, DataType.STRING);

        @Override
        public String value() {
            return "foobar";
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

    private LocalDataCollectOperation operation;
    private Routing testRouting = new Routing(new HashMap<String, Map<String, Integer>>(1){{
        put(TEST_NODE_ID, new HashMap<String, Integer>());
    }});
    private final static String TEST_NODE_ID = "test";

    class CollectorTestmodule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Functions.class).asEagerSingleton();
        }
    }

    @Before
    public void configure() {
        Injector injector = new ModulesBuilder().add(
                new CollectorTestmodule(),
                new AggregationImplModule()
        ).createInjector();
        Functions functions = injector.getInstance(Functions.class);
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

        assertThat((String) result[0][0], equalTo("foobar"));
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
}
