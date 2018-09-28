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

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TableFunctionApplierTest {

    @Test
    public void testUnnestFunctionsAreApplied() {
        Functions functions = getFunctions();
        ArrayType intArray = new ArrayType(DataTypes.INTEGER);
        FunctionIdent unnestIdent = new FunctionIdent("unnest", Collections.singletonList(intArray));
        TableFunctionImplementation unnest = (TableFunctionImplementation) functions.getQualified(unnestIdent);
        TableFunctionApplier tableFunctionApplier = new TableFunctionApplier(
            Arrays.asList(
                new TableFunctionApplier.Func(unnest, Collections.singletonList(Literal.of(new Object[] { 1, 2, 3 }, intArray)), 0),
                new TableFunctionApplier.Func(unnest, Collections.singletonList(Literal.of(new Object[] { 4, 5 }, intArray)), 1)
            ),
            new int[] { 2 }
        );

        Iterator<Row> iterator = tableFunctionApplier.apply(new Row1(10));
        assertThat(iterator.next().materialize(), is(new Object[]{1, 4, 10}));
        assertThat(iterator.next().materialize(), is(new Object[]{2, 5, 10}));
        assertThat(iterator.next().materialize(), is(new Object[]{3, null, 10}));
        assertThat(iterator.hasNext(), is(false));
    }
}
