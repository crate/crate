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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.data.Offset;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Input;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
import io.crate.planner.operators.LogicalPlan;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Node;
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

    public static SymbolsAssert<? extends Symbol> assertList(List<? extends Symbol> actual) {
        return new SymbolsAssert<>(actual);
    }

    public static NodeAssert assertThat(Node actual) {
        return new NodeAssert(actual);
    }

    public static ProjectionAssert assertThat(Projection actual) {
        return new ProjectionAssert(actual);
    }

    public static SQLResponseAssert assertThat(SQLResponse actual) {
        return new SQLResponseAssert(actual);
    }

    public static SQLErrorAssert assertSQLError(ThrowingCallable callable) {
        return new SQLErrorAssert(catchThrowable(callable));
    }

    public static DocKeyAssert assertThat(DocKeys.DocKey actual) {
        return new DocKeyAssert(actual);
    }

    // isSQL Assertions
    public static SQLAssert<AnalyzedRelation> assertThat(AnalyzedRelation actual) {
        return new SQLAssert<>(actual);
    }

    public static SQLAssert<OrderBy> assertThat(OrderBy actual) {
        return new SQLAssert<>(actual);
    }

    public static SQLAssert<Expression> assertThat(Expression actual) {
        return new SQLAssert<>(actual);
    }

    public static LogicalPlanAssert assertThat(LogicalPlan actual) {
        return new LogicalPlanAssert(actual, null);
    }

    public static JoinPairAssert assertThat(JoinPair actual) {
        return new JoinPairAssert(actual);
    }

    // generic helper methods
    public static <T> Condition<T> toCondition(Consumer<T> consumer) {
        return new Condition<>(t -> {
            consumer.accept(t);
            return true;
        }, "");
    }

    public static <T> Consumer<T> isEqualTo(T expectedValue) {
        return x -> assertThat(x).isEqualTo(expectedValue);
    }

    public static <T> Consumer<T> isNull() {
        return s -> assertThat(s).isNull();
    }

    public static <T> Consumer<T> isNotNull() {
        return s -> assertThat(s).isNotNull();
    }

    public static <T> Consumer<T> exactlyInstanceOf(Class<?> clazz) {
        return s -> assertThat(s).isExactlyInstanceOf(clazz);
    }

    public static Consumer<String> startsWith(String expected) {
        return s -> assertThat(s).startsWith(expected);
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
        return s -> assertThat(s)
            .isReference()
            .hasName(expectedName);
    }

    public static Consumer<Symbol> isReference(String expectedName, DataType<?> expectedType) {
        return s -> assertThat(s)
            .isReference()
            .hasName(expectedName)
            .hasType(expectedType);
    }

    public static Consumer<Symbol> isScopedSymbol(String expectedName) {
        return s -> assertThat(s).isScopedSymbol(expectedName);
    }

    public static Consumer<Symbol> isSQL(String expectedName) {
        return s -> assertThat(s).isSQL(expectedName);
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

    public static void assertSQL(Object obj, String expectedStmt) {
        assertThat(SQLPrinter.print(obj)).as(expectedStmt).isEqualTo(expectedStmt);
    }

    // Node
    public static Consumer<Node> isColumnType(String expectedTypeName) {
        return n -> assertThat(n).isColumnType(expectedTypeName);
    }

    public static Consumer<Node> isColumnType(String expectedTypeName, Integer... expectedParams) {
        return n -> assertThat(n).isColumnType(expectedTypeName, expectedParams);
    }

    public static Consumer<Node> isObjectColumnType(String expectedTypeName,
                                                    Consumer<ColumnPolicy> columnPolicyMatcher) {
        return n -> assertThat(n).isObjectColumnType(expectedTypeName, columnPolicyMatcher);
    }

    public static Consumer<Node> isObjectColumnType(String expectedTypeName,
                                                    Consumer<ColumnPolicy> columnPolicyMatcher,
                                                    Consumer<Iterable<? extends Node>> nestedColumnsMatcher) {
        return n -> assertThat(n).isObjectColumnType(expectedTypeName, columnPolicyMatcher, nestedColumnsMatcher);
    }

    public static Consumer<Node> isCollectionColumnType(String expectedTypeName, Consumer<Node> innerTypeMatcher) {
        return n -> assertThat(n).isCollectionColumnType(expectedTypeName, innerTypeMatcher);
    }

    public static Consumer<Node> isColumnDefinition(String expectedIdent,
                                                    Consumer<Node> columnTypeMatcher) {
        return n -> assertThat(n).isColumnDefinition(expectedIdent, columnTypeMatcher);
    }

    // Projections
    public static Consumer<Projection> isLimitAndOffset(int expectedLimit, int expectedOffset) {
        return p -> assertThat(p).isLimitAndOffset(expectedLimit, expectedOffset);
    }

    // Functions
    @SuppressWarnings("rawtypes")
    public static Function<Scalar, Consumer<Scalar>> isSameInstance() {
        return scalar -> s -> assertThat(s).isSameAs(scalar);
    }

    @SuppressWarnings("rawtypes")
    public static Function<Scalar, Consumer<Scalar>> isNotSameInstance() {
        return scalar -> s -> assertThat(s).isNotSameAs(scalar);
    }
}
