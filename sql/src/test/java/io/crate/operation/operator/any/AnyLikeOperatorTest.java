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

package io.crate.operation.operator.any;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.TransactionContext;
import io.crate.data.Input;
import io.crate.operation.predicate.NotPredicate;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class AnyLikeOperatorTest extends CrateUnitTest {

    private static Symbol normalizeSymbol(String pattern, String... expressions) {
        Literal patternLiteral = Literal.of(pattern);
        Object[] value = new Object[expressions.length];
        for (int i = 0; i < expressions.length; i++) {
            value[i] = expressions[i] == null ? null : new BytesRef(expressions[i]);
        }
        Literal valuesLiteral = Literal.of(new ArrayType(DataTypes.STRING), value);
        AnyLikeOperator impl = (AnyLikeOperator) new AnyLikeOperator.AnyLikeResolver().getForTypes(
            Arrays.asList(patternLiteral.valueType(), valuesLiteral.valueType())
        );

        Function function = new Function(
            impl.info(),
            Arrays.<Symbol>asList(patternLiteral, valuesLiteral)
        );
        return impl.normalizeSymbol(function, new TransactionContext(SessionContext.create()));
    }

    private Boolean anyLikeNormalize(String pattern, String... expressions) {
        return (Boolean) ((Literal) normalizeSymbol(pattern, expressions)).value();
    }

    private Boolean anyLike(String pattern, String... expressions) {
        Literal patternLiteral = Literal.of(pattern);
        Object[] value = new Object[expressions.length];
        for (int i = 0; i < expressions.length; i++) {
            value[i] = expressions[i] == null ? null : new BytesRef(expressions[i]);
        }
        Literal valuesLiteral = Literal.of(new ArrayType(DataTypes.STRING), value);
        AnyLikeOperator impl = (AnyLikeOperator) new AnyLikeOperator.AnyLikeResolver().getForTypes(
            Arrays.asList(DataTypes.STRING, valuesLiteral.valueType())
        );

        return impl.evaluate(patternLiteral, valuesLiteral);
    }

    @Test
    public void testNormalizeSingleSymbolEqual() {
        assertTrue(anyLikeNormalize("foo", "foo"));
        assertFalse(anyLikeNormalize("notFoo", "foo"));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertTrue(anyLikeNormalize("%bar", "foobar", "bar"));
        assertTrue(anyLikeNormalize("%bar", "bar"));
        assertFalse(anyLikeNormalize("%bar", "ar", "car"));
        assertTrue(anyLikeNormalize("foo%", "foobar", "kuhbar"));
        assertTrue(anyLikeNormalize("foo%", "foo", "kuh"));
        assertFalse(anyLikeNormalize("foo%", "fo", "kuh"));
        assertTrue(anyLikeNormalize("%oob%", "foobar"));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertTrue(anyLikeNormalize("_ar", "bar"));
        assertFalse(anyLikeNormalize("_bar", "bar"));
        assertTrue(anyLikeNormalize("fo_", "foo", "for"));
        assertTrue(anyLikeNormalize("foo_", "foo", "foot"));
        assertFalse(anyLikeNormalize("foo_", "foo"));
        assertTrue(anyLikeNormalize("_o_", "foo"));
        assertFalse(anyLikeNormalize("_foobar_", "foobar"));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertTrue(anyLikeNormalize("%o_ar", "foobar", "foobaz"));
        assertTrue(anyLikeNormalize("%a_", "foobar"));
        assertTrue(anyLikeNormalize("%o_a%", "foobar"));
        assertTrue(anyLikeNormalize("%i%m%", "Lorem ipsum dolor..."));
        assertTrue(anyLikeNormalize("%%%sum%%", "Lorem ipsum dolor..."));
        assertFalse(anyLikeNormalize("%i%m", "Lorem ipsum dolor..."));
    }

    @Test
    public void testEvaluateStraight() throws Exception {
        assertTrue(anyLike("foo", "foo", "koo", "doo"));
        assertTrue(anyLike("foo", "foo"));
        assertFalse(anyLike("foo"));
        assertFalse(anyLike("foo", "koo", "doo"));
    }

    @Test
    public void testEvaluateLikeMixed() {
        assertTrue(anyLike("%o_ar", "foobar", "foobaz"));
        assertTrue(anyLike("%a_", "foobar"));
        assertTrue(anyLike("%o_a%", "foobar"));
        assertTrue(anyLike("%i%m%", "Lorem ipsum dolor..."));
        assertTrue(anyLike("%%%sum%%", "Lorem ipsum dolor..."));
        assertFalse(anyLike("%i%m", "Lorem ipsum dolor..."));
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertNull(anyLike(null, (String) null));
        assertNull(anyLike("foo", (String) null));
        assertNull(anyLike(null, "bar"));
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNull(anyLikeNormalize(null, (String) null));
        assertNull(anyLikeNormalize("foo", (String) null));
        assertNull(anyLikeNormalize(null, "bar"));
    }

    @Test
    public void testNegateLike() throws Exception {
        Literal patternLiteral = Literal.of("A");
        Literal valuesLiteral = Literal.of(new ArrayType(DataTypes.STRING),
            new Object[]{new BytesRef("A"), new BytesRef("B")});
        FunctionImplementation impl = new AnyLikeOperator.AnyLikeResolver().getForTypes(
            Arrays.asList(DataTypes.STRING, valuesLiteral.valueType())
        );
        Function anyLikeFunction = new Function(impl.info(), Arrays.<Symbol>asList(patternLiteral, valuesLiteral));
        Input<Boolean> normalized = (Input<Boolean>) impl.normalizeSymbol(anyLikeFunction, new TransactionContext(SessionContext.create()));
        assertThat(normalized.value(), is(true));
        assertThat(new NotPredicate().evaluate(normalized), is(false));
    }

}
