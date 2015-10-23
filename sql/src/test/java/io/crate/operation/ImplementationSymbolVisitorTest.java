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

package io.crate.operation;

import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.RowN;
import io.crate.metadata.*;
import io.crate.operation.aggregation.FunctionExpression;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.AverageAggregation;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.collect.CollectExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class ImplementationSymbolVisitorTest extends CrateUnitTest {

    private ImplementationSymbolVisitor visitor;

    static class MultiplyFunction extends Scalar<Long, Object> {

        public final static String NAME = "dummy_multiply";
        public static FunctionInfo INFO = new FunctionInfo(
                new FunctionIdent(NAME, Arrays.<DataType>asList(DataTypes.LONG)),
                DataTypes.LONG
        );

        private AtomicBoolean compiled = new AtomicBoolean(false);


        @Override
        public Long evaluate(Input<Object>... args) {
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

        @Override
        public Scalar<Long, Object> compile(List<Symbol> arguments) {
            compiled.set(true);
            return this;
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

        visitor = new ImplementationSymbolVisitor(injector.getInstance(Functions.class));
    }

    @Test
    public void testAggregationSymbolsInputReuse() throws Exception {
        FunctionInfo countInfo = new FunctionInfo(
                new FunctionIdent(CountAggregation.NAME, Arrays.<DataType>asList(DataTypes.STRING)), DataTypes.LONG);
        FunctionInfo avgInfo = new FunctionInfo(
                new FunctionIdent(AverageAggregation.NAME, Arrays.<DataType>asList(DataTypes.INTEGER)), DataTypes.DOUBLE);

        List<Symbol> aggregations = Arrays.<Symbol>asList(
                Aggregation.finalAggregation(avgInfo, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER),
                Aggregation.finalAggregation(countInfo, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER)
        );

        ImplementationSymbolVisitor.Context context = visitor.extractImplementations(aggregations);
        Input<?> inputCount = context.aggregations.get(0).inputs()[0];
        Input<?> inputAverage = context.aggregations.get(1).inputs()[0];

        assertSame(inputCount, inputAverage);
    }

    @Test
    public void testProcessGroupByProjectionSymbols() throws Exception {
        // select x, y * 2 ... group by x, y * 2

        // keys: [ in(0), multiply(in(1)) ]
        Function multiply = new Function(
                MultiplyFunction.INFO, Arrays.<Symbol>asList(new InputColumn(1))
        );
        List<Symbol> keys = Arrays.asList(new InputColumn(0, DataTypes.LONG), multiply);

        ImplementationSymbolVisitor.Context context = visitor.extractImplementations(keys);
        assertThat(context.collectExpressions().size(), is(2));

        // keyExpressions: [ in0, in1 ]
        CollectExpression[] keyExpressions = context.collectExpressions().toArray(new CollectExpression[2]);

        RowN row = new RowN(new Object[]{1L, 2L});
        keyExpressions[0].setNextRow(row);
        keyExpressions[1].setNextRow(row);
        assertThat((Long) keyExpressions[0].value(), is(1L));
        assertThat((Long) keyExpressions[1].value(), is(2L)); // raw input value

        // inputs: [ x, multiply ]
        List<Input<?>> inputs = context.topLevelInputs();

        assertThat(inputs.size(), is(2));
        assertThat((Long)inputs.get(0).value(), is(1L));
        assertThat((Long) inputs.get(1).value(), is(4L));  // multiplied value
    }

    @Test
    public void testProcessGroupByProjectionSymbolsAggregation() throws Exception {
        // select count(x), x, y * 2 ... group by x, y * 2

        // keys: [ in(0), multiply(in(1)) ]
        Function multiply = new Function(
                MultiplyFunction.INFO, Arrays.<Symbol>asList(new InputColumn(1))
        );
        List<Symbol> keys = Arrays.asList(new InputColumn(0, DataTypes.LONG), multiply);


        // values: [ count(in(0)) ]
        List<Aggregation> values = Arrays.asList(Aggregation.partialAggregation(
                new FunctionInfo(new FunctionIdent(CountAggregation.NAME, Arrays.<DataType>asList(DataTypes.LONG)), DataTypes.LONG),
                DataTypes.LONG,
                Arrays.<Symbol>asList(new InputColumn(0))
        ));

        ImplementationSymbolVisitor.Context context = visitor.extractImplementations(keys);
        // inputs: [ x, multiply ]
        List<Input<?>> keyInputs = context.topLevelInputs();

        for (Aggregation value : values) {
            visitor.process(value, context);
        }

        AggregationContext[] aggregations = context.aggregations();
        assertThat(aggregations.length, is(1));

        // collectExpressions: [ in0, in1 ]
        assertThat(context.collectExpressions().size(), is(2));

        List<Input<?>> allInputs = context.topLevelInputs();
        assertThat(allInputs.size(), is(2)); // only 2 because count is no input

        CollectExpression[] collectExpressions = context.collectExpressions().toArray(new CollectExpression[2]);
        RowN row = new RowN(new Object[]{1L, 2L});
        collectExpressions[0].setNextRow(row);
        collectExpressions[1].setNextRow(row);
        assertThat((Long) collectExpressions[0].value(), is(1L));
        assertThat((Long) collectExpressions[1].value(), is(2L)); // raw input value


        assertThat(keyInputs.size(), is(2));
        assertThat((Long)keyInputs.get(0).value(), is(1L));
        assertThat((Long) keyInputs.get(1).value(), is(4L));  // multiplied value
    }

    @Test
    public void testCompiled() throws Exception {
        Function multiply = new Function(
                MultiplyFunction.INFO, Arrays.<Symbol>asList(new InputColumn(0))
        );
        ImplementationSymbolVisitor.Context context = visitor.extractImplementations(Arrays.asList(multiply));
        assertThat(context.topLevelInputs().get(0), is(instanceOf(FunctionExpression.class)));
        FunctionExpression expression = (FunctionExpression) context.topLevelInputs().get(0);
        Field f = expression.getClass().getDeclaredField("functionImplementation");
        f.setAccessible(true);
        FunctionImplementation impl = (FunctionImplementation)f.get(expression);
        assertThat(impl, is(instanceOf(MultiplyFunction.class)));
        assertThat(((MultiplyFunction)impl).compiled.get(), is(true));
    }
}
