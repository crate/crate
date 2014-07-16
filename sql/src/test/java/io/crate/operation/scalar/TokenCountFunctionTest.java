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

package io.crate.operation.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.operation.Input;
import io.crate.operation.reference.doc.lucene.TokenCountCollectorExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TokenCountFunctionTest {

    private Functions functions;

    private final static DataType intArray = new ArrayType(DataTypes.INTEGER);

    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder().add(new ScalarFunctionModule())
                .createInjector()
                .getInstance(Functions.class);
    }

    @Test
    public void testEvaluateWithNull() throws Exception {
        assertThat(evaluate(null, intArray), nullValue());
    }

    @Test
    public void testNormalizeWithRefs() throws Exception {
        TokenCountFunction tokenCountFunction = getFunction(intArray);
        Function function = new Function(tokenCountFunction.info(), Arrays.<Symbol>asList(
                TestingHelpers.createReference("foo", intArray)));

        Symbol symbol = tokenCountFunction.normalizeSymbol(function);
        assertThat(symbol, Matchers.instanceOf(Reference.class));
        ReferenceIdent ident = ((Reference) symbol).info().ident();

        assertThat(ident.columnIdent().name(), is(TokenCountCollectorExpression.BASE_NAME));
        assertThat(ident.columnIdent().path().size(), is(1));
        assertThat(ident.columnIdent().path().get(0), is("foo"));
    }

    @Test
    public void testNormalizeWithSysRef() throws Exception {
        TokenCountFunction tokenCountFunction = getFunction(intArray);
        Function function = new Function(tokenCountFunction.info(), Arrays.<Symbol>asList(
                new Reference(new ReferenceInfo(
                        new ReferenceIdent(SysClusterTableInfo.IDENT, "foo"), RowGranularity.DOC, intArray))));

        Symbol symbol = tokenCountFunction.normalizeSymbol(function);
        assertThat(symbol, Matchers.sameInstance((Symbol) function));
    }

    @Test
    public void testNormalizeWithNullLiteral() throws Exception {
        assertThat(normalize(Literal.newLiteral(intArray, null)), nullValue());
    }

    @Test
    public void testNormalizeWithIntArrayLiteral() throws Exception {
        assertThat(normalize(Literal.newLiteral(intArray, new Integer[] { 10, 20 })), is(2));
    }

    private Integer normalize(Literal<Object> objectLiteral) {
        TokenCountFunction tokenCountFunction = getFunction(objectLiteral.valueType());
        Function function = new Function(tokenCountFunction.info(), Arrays.<Symbol>asList(
                objectLiteral
        ));
        Symbol symbol = tokenCountFunction.normalizeSymbol(function);
        assertThat(symbol, Matchers.instanceOf(Literal.class));
        return ((Integer) ((Literal) symbol).value());
    }

    @Test
    public void testEvaluate() throws Exception {
        assertThat(evaluateArrayAndList(new Integer[]{10, 20, 30, 40}), is(4));
        assertThat(evaluateArrayAndList(new Integer[]{10, 30, 30, 40}), is(4));
    }

    private Integer evaluateArrayAndList(Integer[] integers) {
        Integer result1 = evaluate(integers, intArray);
        Integer result2 = evaluate(Arrays.asList(integers), intArray);
        assertThat(result1, is(result2));
        return result1;
    }

    @SuppressWarnings("unchecked")
    private Integer evaluate(final Object value, DataType dataType) {
        return getFunction(dataType).evaluate(new Input<Object>() {
            @Override
            public Object value() {
                return value;
            }
        });
    }

    private TokenCountFunction getFunction(DataType dataType) {
        return (TokenCountFunction) functions.get(
                new FunctionIdent(TokenCountFunction.NAME, ImmutableList.of(dataType)));
    }
}
