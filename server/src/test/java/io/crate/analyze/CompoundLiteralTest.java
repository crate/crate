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

package io.crate.analyze;

import io.crate.common.collections.MapBuilder;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.UndefinedType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isFunction;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class CompoundLiteralTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions expressions;

    @Before
    public void prepare() {
        expressions = new SqlExpressions(T3.sources(clusterService));
    }

    @Test
    public void testObjectConstruction() throws Exception {
        Symbol s = expressions.asSymbol("{}");
        assertThat(s, instanceOf(Literal.class));
        Literal l = (Literal) s;
        assertThat(l.value(), is(new HashMap<String, Object>()));

        Literal objectLiteral = (Literal) expressions.normalize(expressions.asSymbol("{ident='value'}"));
        assertThat(objectLiteral.symbolType(), is(SymbolType.LITERAL));
        assertThat(objectLiteral.valueType().id(), is(ObjectType.ID));
        assertThat(objectLiteral.value(), is(Map.of("ident", "value")));

        Literal multipleObjectLiteral = (Literal) expressions.normalize(expressions.asSymbol("{\"Ident\"=123.4, a={}, ident='string'}"));
        Map<String, Object> values = (Map<String, Object>) multipleObjectLiteral.value();
        assertThat(values, is(MapBuilder.<String, Object>newMapBuilder()
            .put("Ident", 123.4d)
            .put("a", new HashMap<String, Object>())
            .put("ident", "string")
            .map()));
    }

    @Test
    public void testObjectConstructionWithExpressionsAsValues() throws Exception {
        Literal objectLiteral = (Literal) expressions.normalize(expressions.asSymbol("{name = 1 + 2}"));
        assertThat(objectLiteral.symbolType(), is(SymbolType.LITERAL));
        assertThat(objectLiteral.value(), is(Map.<String, Object>of("name", 3)));

        Literal nestedObjectLiteral = (Literal) expressions.normalize(expressions.asSymbol("{a = {name = concat('foo', 'bar')}}"));
        @SuppressWarnings("unchecked") Map<String, Object> values = (Map<String, Object>) nestedObjectLiteral.value();
        assertThat(values, is(Map.of("a", Map.of("name", "foobar"))));
    }

    private Symbol analyzeExpression(String expression) {
        return expressions.normalize(expressions.asSymbol(expression));
    }

    @Test
    public void testObjectConstructionWithParameterExpression() throws Exception {
        assertThat(expressions.asSymbol("{ident=?}"), isFunction("_map"));
    }

    @Test
    public void testObjectConstructionFailsOnDuplicateKeys() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Object literal cannot contain duplicate keys (`a`)");
        analyzeExpression("{a=1, a=2}");
    }

    @Test
    public void testArrayConstructionWithOnlyLiterals() throws Exception {
        Literal<?> emptyArray = (Literal<?>) analyzeExpression("[]");
        assertThat((List<Object>) emptyArray.value(), Matchers.empty());
        assertThat(emptyArray.valueType(), is(new ArrayType<>(UndefinedType.INSTANCE)));

        Literal<?> singleArray = (Literal<?>) analyzeExpression("[1]");
        assertThat(singleArray.valueType(), is(new ArrayType<>(DataTypes.INTEGER)));
        assertThat(((List<Long>) singleArray.value()), contains(1));

        Literal<?> multiArray = (Literal<?>) analyzeExpression("[1, 2, 3]");
        assertThat(multiArray.valueType(), is(new ArrayType<>(DataTypes.INTEGER)));
        assertThat(((List<Long>) multiArray.value()), contains(1, 2, 3));
    }

    @Test
    public void testArrayConstructionWithParameterExpression() throws Exception {
        Symbol array = expressions.asSymbol("[1, ?]");
        assertThat(array, isFunction("_array"));
        assertThat(((io.crate.expression.symbol.Function) array).arguments().size(), is(2));
    }

    @Test
    public void testArrayDifferentTypes() {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `'string'` of type `text` to type `integer`");
        analyzeExpression("[1, 'string']");
    }

    @Test
    public void testNestedArrayLiteral() throws Exception {
        Map<String, DataType<?>> expected = Map.of(
            "'string'", DataTypes.STRING,
            "0", DataTypes.INTEGER,
            "1.8", DataTypes.DOUBLE,
            "TRUE", DataTypes.BOOLEAN
        );
        for (Map.Entry<String, DataType<?>> entry : expected.entrySet()) {
            Symbol nestedArraySymbol = analyzeExpression("[[" + entry.getKey() + "]]");
            assertThat(nestedArraySymbol, Matchers.instanceOf(Literal.class));
            Literal<?> nestedArray = (Literal<?>) nestedArraySymbol;
            assertThat(nestedArray.valueType(), is(new ArrayType<>(new ArrayType<>(entry.getValue()))));
        }
    }
}
