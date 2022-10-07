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

package io.crate.testing;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.data.Offset;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.function.Executable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Input;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class Asserts extends Assertions {

    private Asserts() {}

    public static SettingsAssert assertThat(Settings actual) {
        return new SettingsAssert(actual);
    }

    public static SymbolAssert assertThat(Symbol actual) {
        return new SymbolAssert(actual);
    }

    public static <T> Condition<T> toCondition(Consumer<T> consumer) {
        return new Condition<>(t -> {
            consumer.accept(t);
            return true;
        }, "");
    }

    public static Consumer<Symbol> isNull() {
        return s -> assertThat(s).isNull();
    }

    public static Consumer<Symbol> isNotNull() {
        return s -> assertThat(s).isNotNull();
    }

    public static Consumer<String> startsWith(String expected) {
        return s -> assertThat(s).startsWith(expected);
    }

    public static <T> Consumer<T> exactlyInstanceOf(Class<?> clazz) {
        return s -> assertThat(s).isExactlyInstanceOf(clazz);
    }

    // Table
    public static Consumer<AnalyzedRelation> isDocTable(RelationName expectedRelationName) {
        return s -> {
            assertThat(s).isExactlyInstanceOf(DocTableRelation.class);
            assertThat(((DocTableRelation) s).tableInfo().ident())
                .as("relationName").isEqualTo(expectedRelationName);
        };
    }

    // Symbol related
    public static Consumer<Symbol> isField(String expectedColumnName) {
        return s -> Asserts.assertThat(s).isField(expectedColumnName);
    }

    public static Consumer<Symbol> isField(String expectedColumnName, DataType<?> expectedDataType) {
        return s -> Asserts.assertThat(s).isField(expectedColumnName, expectedDataType);
    }

    public static Consumer<Symbol> isField(String expectedColumnName, RelationName expectedRelationName) {
        return s -> Asserts.assertThat(s).isField(expectedColumnName, expectedRelationName);
    }

    public static Consumer<Symbol> isLiteral(Object value) {
        return s -> Asserts.assertThat(s).isLiteral(value);
    }

    public static Consumer<Symbol> isLiteral(Object expectedValue, DataType<?> expectedType) {
        return s -> assertThat(s).isLiteral(expectedValue, expectedType);
    }

    public static Consumer<Symbol> isLiteral(Double expectedValue, double precisionError) {
        return s -> {
            assertThat(s).isNotNull();
            assertThat(s).isExactlyInstanceOf(Literal.class);
            assertThat(s).hasDataType(DataTypes.DOUBLE);
            Double value = (Double) ((Input<?>) s).value();
            assertThat(value).isCloseTo(expectedValue, Offset.offset(precisionError));
        };
    }

    public static Consumer<Symbol> isReference(String expectedName) {
        return s -> assertThat(s).isReference(expectedName);
    }

    public static Consumer<Symbol> isReference(String expectedName, DataType<?> expectedType) {
        return s -> assertThat(s).isReference(expectedName, expectedType);
    }

    public static Consumer<Symbol> isFetchStub(String expectedName) {
        return s -> assertThat(s).isFetchStub(expectedName);
    }

    public static Consumer<Symbol> isFetchMarker(RelationName expectedRelationName, Consumer<Symbol> refsMatcher) {
        return s -> assertThat(s).isFetchMarker(expectedRelationName, refsMatcher);
    }

    public static Consumer<Symbol> isFunction(String expectedName) {
        return s -> assertThat(s).isFunction(expectedName);
    }

    public static Consumer<Symbol> isFunction(String expectedName, List<DataType<?>> expectedArgTypes) {
        return s -> assertThat(s).isFunction(expectedName, expectedArgTypes);
    }

    @SafeVarargs
    public static Consumer<Symbol> isFunction(String expectedName, Consumer<Symbol>... argMatchers) {
        return s -> assertThat(s).isFunction(expectedName, argMatchers);
    }

    public static Consumer<Symbol> isInputColumn(int expectedIndex) {
        return s -> assertThat(s).isInputColumn(expectedIndex);
    }

    public static Consumer<Symbol> isAlias(String expectedAliasName, Consumer<Symbol> childMatcher) {
        return s -> assertThat(s).isAlias(expectedAliasName, childMatcher);
    }

    @SuppressWarnings("rawtypes")
    public static Function<Scalar, Consumer<Scalar>> isSameInstance() {
        return scalar -> s -> assertThat(s).isSameAs(scalar);
    }

    @SuppressWarnings("rawtypes")
    public static Function<Scalar, Consumer<Scalar>> isNotSameInstance() {
        return scalar -> s -> assertThat(s).isNotSameAs(scalar);
    }

    public static void assertThrowsMatches(Executable executable, Matcher<? super Throwable> matcher) {
        try {
            executable.execute();
            fail("Expected exception to be thrown, but nothing was thrown.");
        } catch (Throwable t) {
            org.hamcrest.MatcherAssert.assertThat(t, matcher);
        }
    }

    public static void assertThrowsMatches(Executable executable, Class<? extends Throwable> type, String msgSubString) {
        assertThrowsMatches(executable, type, msgSubString,"Expected exception to be thrown, but nothing was thrown.");
    }

    public static void assertThrowsMatches(Executable executable,
                                           Class<? extends Throwable> type,
                                           String msgSubString,
                                           String assertionFailMsg) {
        try {
            executable.execute();
            fail(assertionFailMsg);
        } catch (Throwable t) {
            MatcherAssert.assertThat(t, instanceOf(type));
            MatcherAssert.assertThat(t.getMessage(), containsString(msgSubString));
        }
    }
}
