/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.pipeline;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.pipeline.RowTransformingBatchIterator;
import io.crate.metadata.FunctionInfo;
import io.crate.expression.InputFactory;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.TestingBatchIterators;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.crate.testing.TestingHelpers.getFunctions;

public class RowTransformingBatchIteratorTest extends CrateUnitTest {

    private List<Input<?>> inputs;
    private List<CollectExpression<Row, ?>> expressions;

    private List<Object[]> expectedResult = LongStream.range(0, 10)
        .mapToObj(l -> new Object[] { l + 2L })
        .collect(Collectors.toList());

    @Before
    public void createInputs() throws Exception {
        InputFactory inputFactory = new InputFactory(getFunctions());
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns();
        inputs = Collections.singletonList(ctx.add(ArithmeticFunctions.of(
            ArithmeticFunctions.Names.ADD,
            new InputColumn(0, DataTypes.LONG),
            Literal.of(2L),
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT
        )));
        expressions = ctx.expressions();
    }

    @Test
    public void testRowTransformingIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new RowTransformingBatchIterator(TestingBatchIterators.range(0, 10),
                inputs,
                expressions)
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
