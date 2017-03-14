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

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.*;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.operation.aggregation.FunctionExpression;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;

public class InputFactoryTest extends CrateUnitTest {

    private SqlExpressions expressions = new SqlExpressions(ImmutableMap.of(T3.T1, T3.TR_1), T3.TR_1);
    private InputFactory factory = new InputFactory(expressions.functions());

    @Test
    public void testAggregationSymbolsInputReuse() throws Exception {
        Function countX = (Function) expressions.asSymbol("count(x)");
        Function avgX = (Function) expressions.asSymbol("avg(x)");

        List<Symbol> aggregations = Arrays.<Symbol>asList(
            Aggregation.finalAggregation(countX.info(), Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER),
            Aggregation.finalAggregation(avgX.info(), Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER)
        );

        InputFactory.Context<CollectExpression<Row, ?>> ctx = factory.ctxForAggregations();
        ctx.add(aggregations);
        List<AggregationContext> aggregationContexts = ctx.aggregations();

        Input<?> inputCount = aggregationContexts.get(0).inputs()[0];
        Input<?> inputAverage = aggregationContexts.get(1).inputs()[0];

        assertSame(inputCount, inputAverage);
    }

    @Test
    public void testProcessGroupByProjectionSymbols() throws Exception {
        // select x, y * 2 ... group by x, y * 2

        // keys: [ in(0), in(1) + 10 ]
        Function add = AddFunction.of(new InputColumn(1, DataTypes.INTEGER), Literal.of(10));
        List<Symbol> keys = Arrays.asList(new InputColumn(0, DataTypes.LONG), add);

        InputFactory.Context<CollectExpression<Row, ?>> ctx = factory.ctxForAggregations();
        ctx.add(keys);
        ArrayList<CollectExpression<Row, ?>> expressions = new ArrayList<>(ctx.expressions());
        assertThat(expressions.size(), is(2));

        // keyExpressions: [ in0, in1 ]

        RowN row = new RowN(new Object[]{1L, 2L});
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        assertThat((Long) expressions.get(0).value(), is(1L));
        assertThat((Long) expressions.get(1).value(), is(2L)); // raw input value

        // inputs: [ x, add ]
        List<Input<?>> inputs = ctx.topLevelInputs();

        assertThat(inputs.size(), is(2));
        assertThat((Long) inputs.get(0).value(), is(1L));
        assertThat((Long) inputs.get(1).value(), is(12L));  // + 10
    }

    @Test
    public void testProcessGroupByProjectionSymbolsAggregation() throws Exception {
        // select count(x), x, y * 2 ... group by x, y * 2

        // keys: [ in(0), in(1) + 10 ]
        Function add = AddFunction.of(new InputColumn(1, DataTypes.INTEGER), Literal.of(10));
        List<Symbol> keys = Arrays.asList(new InputColumn(0, DataTypes.LONG), add);

        Function countX = (Function) expressions.asSymbol("count(x)");

        // values: [ count(in(0)) ]
        List<Aggregation> values = Arrays.asList(Aggregation.partialAggregation(
            countX.info(),
            countX.valueType(),
            Arrays.<Symbol>asList(new InputColumn(0))
        ));

        InputFactory.Context<CollectExpression<Row, ?>> ctx = factory.ctxForAggregations();
        ctx.add(keys);

        // inputs: [ x, add ]
        List<Input<?>> keyInputs = ctx.topLevelInputs();

        ctx.add(values);

        List<AggregationContext> aggregations = ctx.aggregations();
        assertThat(aggregations.size(), is(1));

        // collectExpressions: [ in0, in1 ]
        List<CollectExpression<Row, ?>> expressions = new ArrayList<>(ctx.expressions());
        assertThat(expressions.size(), is(2));

        List<Input<?>> allInputs = ctx.topLevelInputs();
        assertThat(allInputs.size(), is(2)); // only 2 because count is no input

        RowN row = new RowN(new Object[]{1L, 2L});
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        assertThat((Long) expressions.get(0).value(), is(1L));
        assertThat((Long) expressions.get(1).value(), is(2L)); // raw input value

        assertThat(keyInputs.size(), is(2));
        assertThat((Long) keyInputs.get(0).value(), is(1L));
        assertThat((Long) keyInputs.get(1).value(), is(12L));  // 2 + 10
    }

    @Test
    public void testCompiled() throws Exception {
        Function function = (Function) expressions.normalize(expressions.asSymbol("a like 'f%'"));
        InputFactory.Context<Input<?>> ctx = factory.ctxForRefs(i -> Literal.of("foo"));
        Input<?> input = ctx.add(function);

        FunctionExpression expression = (FunctionExpression) input;
        java.lang.reflect.Field f = FunctionExpression.class.getDeclaredField("functionImplementation");
        f.setAccessible(true);
        FunctionImplementation impl = (FunctionImplementation) f.get(expression);
        assertThat(impl.info(), is(function.info()));

        FunctionIdent ident = function.info().ident();
        FunctionImplementation uncompiled =
            expressions.functions().get(ident.schema(), ident.name(), ident.argumentTypes());
        assertThat(uncompiled, not(sameInstance(impl)));
    }
}
