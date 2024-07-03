/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.pipeline;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.Scalar.Feature;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class MapRowUsingInputsTest extends ESTestCase {

    private List<Input<?>> inputs;
    private List<CollectExpression<Row, ?>> expressions;
    private final TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Before
    public void createInputs() throws Exception {
        InputFactory inputFactory = new InputFactory(createNodeContext());
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
        var addFunction = new Function(
            Signature.builder(ArithmeticFunctions.Names.ADD, FunctionType.SCALAR)
                .argumentTypes(DataTypes.LONG.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature())
                .returnType(DataTypes.LONG.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.COMPARISON_REPLACEMENT, Scalar.Feature.NULLABLE)
                .build(),
            List.of(new InputColumn(0, DataTypes.LONG), Literal.of(2L)),
            DataTypes.LONG
        );
        inputs = Collections.singletonList(ctx.add(addFunction));
        expressions = ctx.expressions();
    }

    @Test
    public void testAdd2IsAppliedToFirstColumnOfArgumentRow() throws Exception {
        MapRowUsingInputs mapRowUsingInputs = new MapRowUsingInputs(inputs, expressions);
        Row result = mapRowUsingInputs.apply(new Row1(2L));
        assertThat(result.numColumns()).isEqualTo(1);
        assertThat(result.get(0)).isEqualTo(4L);
    }

    @Test
    public void testMapRowUsingInputsUsesASharedRow() {
        MapRowUsingInputs mapRowUsingInputs = new MapRowUsingInputs(inputs, expressions);
        Row fst = mapRowUsingInputs.apply(new Row1(2L));
        Row snd = mapRowUsingInputs.apply(new Row1(2L));
        assertThat(fst).isSameAs(snd);
    }
}
