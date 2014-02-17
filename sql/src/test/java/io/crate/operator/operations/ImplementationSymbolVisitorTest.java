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

package io.crate.operator.operations;

import io.crate.metadata.*;
import io.crate.operator.Input;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.CountAggregation;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ImplementationSymbolVisitorTest {

    private ImplementationSymbolVisitor visitor;

    static class MultiplyFunction implements Scalar<Long> {

        public final static String NAME = "dummy_multiply";
        public static FunctionInfo INFO = new FunctionInfo(
                new FunctionIdent(NAME, Arrays.asList(DataType.LONG)),
                DataType.LONG
        );

        @Override
        public Long evaluate(Input<?>... args) {
            return (Long)args[0].value() * 2L;
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            throw new UnsupportedOperationException();
        }
    }

    class TestScalarFunctionModule extends AbstractModule {
        @Override
        protected void configure() {
            MapBinder<FunctionIdent, FunctionImplementation> functionBinder =
                    MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
            functionBinder.addBinding(MultiplyFunction.INFO.ident()).to(MultiplyFunction.class);
        }
    }

    @Before
    public void setupVisitor() {
        Injector injector = new ModulesBuilder().add(
                new AggregationImplModule(),
                new TestScalarFunctionModule()
        ).createInjector();

        visitor = new ImplementationSymbolVisitor(
                null,
                injector.getInstance(Functions.class),
                RowGranularity.DOC
        );
    }

    @Test
    public void testProcessGroupByProjectionSymbols() throws Exception {
        // select x, y * 2 ... group by x, y * 2

        // keys: [ in(0), multiply(in(1)) ]
        Function multiply = new Function(
                MultiplyFunction.INFO, Arrays.<Symbol>asList(new InputColumn(1))
        );
        List<Symbol> keys = Arrays.asList(new InputColumn(0), multiply);

        ImplementationSymbolVisitor.Context context = visitor.process(keys);
        assertThat(context.collectExpressions().size(), is(2));

        // keyExpressions: [ in0, in1 ]
        CollectExpression[] keyExpressions = context.collectExpressions().toArray(new CollectExpression[2]);
        keyExpressions[0].setNextRow(1L, 2L);
        keyExpressions[1].setNextRow(1L, 2L);
        assertThat((Long) keyExpressions[0].value(), is(1L));
        assertThat((Long) keyExpressions[1].value(), is(2L)); // raw input value

        // inputs: [ x, multiply ]
        Input<?>[] inputs = context.topLevelInputs();

        assertThat(inputs.length, is(2));
        assertThat((Long)inputs[0].value(), is(1L));
        assertThat((Long) inputs[1].value(), is(4L));  // multiplied value
    }

    @Test
    public void testProcessGroupByProjectionSymbolsAggregation() throws Exception {
        // select count(x), x, y * 2 ... group by x, y * 2

        // keys: [ in(0), multiply(in(1)) ]
        Function multiply = new Function(
                MultiplyFunction.INFO, Arrays.<Symbol>asList(new InputColumn(1))
        );
        List<Symbol> keys = Arrays.asList(new InputColumn(0), multiply);


        // values: [ count(in(0)) ]
        List<Aggregation> values = Arrays.asList(new Aggregation(
                new FunctionInfo(new FunctionIdent(CountAggregation.NAME, Arrays.asList(DataType.LONG)), DataType.LONG),
                Arrays.<Symbol>asList(new InputColumn(0)),
                Aggregation.Step.ITER,
                Aggregation.Step.PARTIAL
        ));

        ImplementationSymbolVisitor.Context context = visitor.process(keys);
        // inputs: [ x, multiply ]
        Input<?>[] keyInputs = context.topLevelInputs();

        for (Aggregation value : values) {
            visitor.process(value, context);
        }

        AggregationContext[] aggregations = context.aggregations();
        assertThat(aggregations.length, is(1));

        // collectExpressions: [ in0, in1 ]
        assertThat(context.collectExpressions().size(), is(2));

        Input<?>[] allInputs = context.topLevelInputs();
        assertThat(allInputs.length, is(2)); // only 2 because count is no input

        CollectExpression[] collectExpressions = context.collectExpressions().toArray(new CollectExpression[2]);
        collectExpressions[0].setNextRow(1L, 2L);
        collectExpressions[1].setNextRow(1L, 2L);
        assertThat((Long) collectExpressions[0].value(), is(1L));
        assertThat((Long) collectExpressions[1].value(), is(2L)); // raw input value


        assertThat(keyInputs.length, is(2));
        assertThat((Long)keyInputs[0].value(), is(1L));
        assertThat((Long) keyInputs[1].value(), is(4L));  // multiplied value
    }
}
