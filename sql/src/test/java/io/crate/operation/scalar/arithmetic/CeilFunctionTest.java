/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.scalar.arithmetic;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CeilFunctionTest extends AbstractScalarFunctionsTest {

    private CeilFunction getFunction(DataType type) {
        return (CeilFunction) functions.get(new FunctionIdent(CeilFunction.NAME, Arrays.asList(type)));
    }

    private Number evaluate(Number number, DataType type) {
        return getFunction(type).evaluate((Input) Literal.newLiteral(type, number));
    }

    private Symbol normalize(Number number, DataType type) {
        CeilFunction function = getFunction(type);
        return function.normalizeSymbol(new Function(function.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(type, number))));
    }

    @Test
    public void testEvaluateOnDouble() throws Exception {
        assertThat((Long) evaluate(29.9, DataTypes.DOUBLE), is(30L));
        assertThat(evaluate(null, DataTypes.DOUBLE), nullValue());
    }

    @Test
    public void testEvaluateOnFloat() throws Exception {
        assertThat((Integer) evaluate(29.9f, DataTypes.FLOAT), is(30));
        assertThat(evaluate(null, DataTypes.FLOAT), nullValue());
    }

    @Test
    public void testEvaluateOnIntAndLong() throws Exception {
        assertThat((Integer) evaluate(20, DataTypes.INTEGER), is(20));
        assertThat(evaluate(null, DataTypes.INTEGER), nullValue());
        assertThat((Long) evaluate(20L, DataTypes.LONG), is(20L));
        assertThat(evaluate(null, DataTypes.LONG), nullValue());
    }

    @Test
    public void testNormalizeValueSymbol() throws Exception {
        assertThat(normalize(42.9, DataTypes.DOUBLE), isLiteral(43L));
        assertThat(normalize(42.9f, DataTypes.FLOAT), isLiteral(43));
        assertThat(normalize(null, DataTypes.FLOAT), isLiteral(null, DataTypes.INTEGER));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        Reference height = createReference("height", DataTypes.DOUBLE);
        CeilFunction round = getFunction(DataTypes.DOUBLE);
        Function function = new Function(round.info(), Arrays.<Symbol>asList(height));
        Function normalized = (Function) round.normalizeSymbol(function);
        assertThat(normalized, Matchers.sameInstance(function));
    }
}