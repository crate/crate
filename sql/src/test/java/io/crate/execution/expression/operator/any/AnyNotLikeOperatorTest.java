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

package io.crate.execution.expression.operator.any;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.TransactionContext;
import io.crate.execution.expression.predicate.NotPredicate;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class AnyNotLikeOperatorTest extends CrateUnitTest {

    private static Symbol normalizeSymbol(String pattern, String... expressions) {
        Literal patternLiteral = Literal.of(pattern);
        Object[] value = new Object[expressions.length];
        for (int i = 0; i < expressions.length; i++) {
            value[i] = expressions[i] == null ? null : new BytesRef(expressions[i]);
        }
        Literal valuesLiteral = Literal.of(new ArrayType(DataTypes.STRING), value);
        AnyNotLikeOperator impl = (AnyNotLikeOperator) new AnyNotLikeOperator.AnyNotLikeResolver().getForTypes(
            Arrays.asList(patternLiteral.valueType(), valuesLiteral.valueType())
        );

        Function function = new Function(
            impl.info(),
            Arrays.<Symbol>asList(patternLiteral, valuesLiteral)
        );
        return impl.normalizeSymbol(function, new TransactionContext(SessionContext.create()));
    }

    private Boolean anyNotLikeNormalize(String pattern, String... expressions) {
        return (Boolean) ((Literal) normalizeSymbol(pattern, expressions)).value();
    }

    private Boolean anyNotLike(String pattern, String... expressions) {
        Literal patternLiteral = Literal.of(pattern);
        Object[] value = new Object[expressions.length];
        for (int i = 0; i < expressions.length; i++) {
            value[i] = expressions[i] == null ? null : new BytesRef(expressions[i]);
        }
        Literal valuesLiteral = Literal.of(new ArrayType(DataTypes.STRING), value);
        AnyNotLikeOperator impl = (AnyNotLikeOperator) new AnyNotLikeOperator.AnyNotLikeResolver().getForTypes(
            Arrays.asList(DataTypes.STRING, valuesLiteral.valueType())
        );

        return impl.evaluate(patternLiteral, valuesLiteral);
    }

    @Test
    public void testNormalizeSingleSymbolEqual() {
        assertFalse(anyNotLikeNormalize("foo", "foo"));
        assertTrue(anyNotLikeNormalize("notFoo", "foo"));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertFalse(anyNotLikeNormalize("%bar", "foobar", "bar"));
        assertFalse(anyNotLikeNormalize("%bar", "bar"));
        assertTrue(anyNotLikeNormalize("%bar", "ar", "car"));
        assertTrue(anyNotLikeNormalize("foo%", "foobar", "kuhbar"));
        assertTrue(anyNotLikeNormalize("foo%", "foo", "kuh"));
        assertTrue(anyNotLikeNormalize("foo%", "fo", "kuh"));
        assertFalse(anyNotLikeNormalize("%oob%", "foobar"));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertFalse(anyNotLikeNormalize("_ar", "bar"));
        assertTrue(anyNotLikeNormalize("_bar", "bar"));
        assertFalse(anyNotLikeNormalize("fo_", "foo", "for"));
        assertTrue(anyNotLikeNormalize("foo_", "foo", "foot"));
        assertTrue(anyNotLikeNormalize("foo_", "foo"));
        assertFalse(anyNotLikeNormalize("_o_", "foo"));
        assertTrue(anyNotLikeNormalize("_foobar_", "foobar"));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertTrue(anyNotLikeNormalize("%o_ar", "foobar", "foobaz"));
        assertFalse(anyNotLikeNormalize("%a_", "foobar"));
        assertFalse(anyNotLikeNormalize("%o_a%", "foobar"));
        assertFalse(anyNotLikeNormalize("%i%m%", "Lorem ipsum dolor..."));
        assertFalse(anyNotLikeNormalize("%%%sum%%", "Lorem ipsum dolor..."));
        assertTrue(anyNotLikeNormalize("%i%m", "Lorem ipsum dolor..."));
    }

    @Test
    public void testEvaluateStraight() throws Exception {
        assertTrue(anyNotLike("foo", "foo", "koo", "doo"));
        assertFalse(anyNotLike("foo", "foo"));
        assertFalse(anyNotLike("foo"));
        assertTrue(anyNotLike("foo", "koo", "doo"));
    }

    @Test
    public void testEvaluateLikeMixed() {
        assertTrue(anyNotLike("%o_ar", "foobar", "foobaz"));
        assertFalse(anyNotLike("%a_", "foobar"));
        assertFalse(anyNotLike("%o_a%", "foobar"));
        assertFalse(anyNotLike("%i%m%", "Lorem ipsum dolor..."));
        assertFalse(anyNotLike("%%%sum%%", "Lorem ipsum dolor..."));
        assertTrue(anyNotLike("%i%m", "Lorem ipsum dolor..."));
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertNull(anyNotLike(null, (String) null));
        assertNull(anyNotLike("foo", (String) null));
        assertNull(anyNotLike(null, "bar"));
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNull(anyNotLikeNormalize(null, (String) null));
        assertNull(anyNotLikeNormalize("foo", (String) null));
        assertNull(anyNotLikeNormalize(null, "bar"));
    }

    @Test
    public void testNegateNotLike() throws Exception {
        Literal patternLiteral = Literal.of("A");
        Literal valuesLiteral = Literal.of(new ArrayType(DataTypes.STRING),
            new Object[]{new BytesRef("A"), new BytesRef("B")});
        FunctionImplementation impl = new AnyNotLikeOperator.AnyNotLikeResolver().getForTypes(
            Arrays.asList(DataTypes.STRING, valuesLiteral.valueType())
        );
        Function anyNotLikeFunction = new Function(impl.info(), Arrays.<Symbol>asList(patternLiteral, valuesLiteral));
        Input<Boolean> normalized = (Input<Boolean>) impl.normalizeSymbol(anyNotLikeFunction, new TransactionContext(SessionContext.create()));
        assertThat(normalized.value(), is(true));
        assertThat(new NotPredicate().evaluate(normalized), is(false));
    }

}
