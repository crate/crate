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

package io.crate.operation.aggregation;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccountingContext;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.types.DataType;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public abstract class AggregationTest {

    protected static final RamAccountingContext ramAccountingContext =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    protected Functions functions;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    class AggregationTestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Functions.class).asEagerSingleton();
        }
    }

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder().add(
                new AggregationTestModule(),
                new AggregationImplModule()
        ).createInjector();

        functions = injector.getInstance(Functions.class);
    }

    public Object[][] executeAggregation(String name, DataType dataType, Object[][] data) throws Exception {

        FunctionIdent fi;
        InputCollectExpression[] inputs;
        if (dataType != null) {
            fi = new FunctionIdent(name, ImmutableList.of(dataType));
            inputs = new InputCollectExpression[]{new InputCollectExpression(0)};
        } else {
            fi = new FunctionIdent(name, ImmutableList.<DataType>of());
            inputs = new InputCollectExpression[0];
        }
        AggregationFunction impl = (AggregationFunction) functions.get(fi);
        Object state = impl.newState(ramAccountingContext);

        for (Object[] row : data) {
            for (InputCollectExpression i : inputs) {
                i.setNextRow(row);
            }
            state = impl.iterate(ramAccountingContext, state, inputs);

        }
        state = impl.terminatePartial(ramAccountingContext, state);
        return new Object[][]{{state}};
    }

}
