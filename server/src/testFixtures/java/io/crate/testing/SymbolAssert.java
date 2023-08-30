/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.ObjectAssert;

import io.crate.data.Input;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.FetchStub;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.types.DataType;

public final class SymbolAssert extends AbstractAssert<SymbolAssert, Symbol> {

    public SymbolAssert(Symbol actual) {
        super(actual, SymbolAssert.class);
    }

    public SymbolAssert hasDataType(DataType<?> expectedType) {
        assertThat(actual.valueType()).as("value type").isEqualTo(expectedType);
        return this;
    }

    public SymbolAssert hasValue(Object expectedValue) {
        Object value = ((Input<?>) actual).value();
        if (expectedValue == null) {
            assertThat(value).isNull();
        } else {
            assertThat(value).isEqualTo(expectedValue);
        }
        return this;
    }

    public SymbolAssert isInputColumn(int expectedIndex) {
        isNotNull();
        isExactlyInstanceOf(InputColumn.class);
        assertThat(((InputColumn) actual).index()).as("index").isEqualTo(expectedIndex);
        return this;
    }

    public SymbolAssert isField(String expectedColumnName) {
        return isField(expectedColumnName, (DataType<?>) null);
    }

    public SymbolAssert isField(final String expectedColumnName, @Nullable final DataType<?> dataType) {
        isNotNull();
        isExactlyInstanceOf(ScopedSymbol.class);
        assertThat(((ScopedSymbol) actual).column().sqlFqn()).as("columnName").isEqualTo(expectedColumnName);
        if (dataType != null) {
            hasDataType(dataType);
        }
        return this;
    }

    public SymbolAssert isField(String expectedColumnName, RelationName expectedRel) {
        isNotNull();
        isExactlyInstanceOf(ScopedSymbol.class);
        assertThat(((ScopedSymbol) actual).column().sqlFqn()).as("columnName").isEqualTo(expectedColumnName);
        assertThat(((ScopedSymbol) actual).relation()).as("relation").isEqualTo(expectedRel);
        return this;
    }

    public SymbolAssert isLiteral(Object expectedValue) {
        return isLiteral(expectedValue, null);
    }

    public SymbolAssert isLiteral(final Object expectedValue, @Nullable final DataType<?> expectedType) {
        isNotNull();
        isExactlyInstanceOf(Literal.class);
        if (expectedType == null) {
            return hasValue(expectedValue);
        }
        hasDataType(expectedType);
        return hasValue(expectedValue);
    }

    public ReferenceAssert isReference() {
        isNotNull();
        isInstanceOf(Reference.class);
        return new ReferenceAssert((Reference) actual);
    }

    public ReferenceAssert isVoidReference() {
        isNotNull();
        isExactlyInstanceOf(VoidReference.class);
        return new ReferenceAssert((Reference) actual);
    }

    public ReferenceAssert isDynamicReference() {
        isNotNull();
        isExactlyInstanceOf(DynamicReference.class);
        return new ReferenceAssert((Reference) actual);
    }

    public SymbolAssert isFetchStub(String expectedColumnName) {
        isNotNull();
        isExactlyInstanceOf(FetchStub.class);
        assertThat(((FetchStub) actual).ref().column().sqlFqn())
            .as("ref sqlFqn")
            .isEqualTo(expectedColumnName);
        return this;
    }

    public SymbolAssert isFetchMarker(RelationName expectedRelationName, Consumer<Symbol> refsMatcher) {
        isNotNull();
        isExactlyInstanceOf(FetchMarker.class);
        assertThat(((FetchMarker) actual).relationName())
            .as("relation name").isEqualTo(expectedRelationName);
        assertThat(((FetchMarker) actual).fetchRefs())
            .as("refs")
            .satisfiesExactly(refsMatcher);
        return this;
    }

    public SymbolAssert isScopedSymbol(String expectedName) {
        isNotNull();
        isInstanceOf(ScopedSymbol.class);
        assertThat(((ScopedSymbol) actual).column().sqlFqn())
            .as("sqlFqn")
            .isEqualTo(expectedName);
        return this;
    }

    public SymbolAssert isFunction(final String expectedName) {
        return isFunction(expectedName, (List<DataType<?>>) null);
    }

    public SymbolAssert isFunction(final String expectedName, @Nullable final List<DataType<?>> expectedArgumentTypes) {
        isNotNull();
        isInstanceOf(Function.class);
        Function f = ((Function) actual);
        assertThat(f.name())
            .as("Function name")
            .isEqualTo(expectedName);

        if (expectedArgumentTypes != null) {
            assertThat(f.arguments()).hasSize(expectedArgumentTypes.size());
            for (int i = 0; i < expectedArgumentTypes.size(); i++) {
                assertThat(f.arguments().get(i).valueType())
                    .as("Argument pos: " + i)
                    .isEqualTo(expectedArgumentTypes.get(i));
            }
        }
        return this;
    }

    @SafeVarargs
    public final SymbolAssert isFunction(final String name, Consumer<Symbol>... argMatchers) {
        isFunction(name);
        if (argMatchers != null) {
            assertThat(((Function) actual).arguments()).satisfiesExactly(argMatchers);
        }
        return this;
    }

    public SymbolAssert isAlias(String expectedAliasName, Consumer<Symbol> childMatcher) {
        isNotNull();
        isExactlyInstanceOf(AliasSymbol.class);
        assertThat(((AliasSymbol) actual).alias()).as("alias").isEqualTo(expectedAliasName);
        assertThat(((AliasSymbol) actual).symbol()).as("child").satisfies(childMatcher);
        return this;
    }

    /**
     * Similar to {@link #isAlias(String, Consumer)} but returns a new {@link SymbolAssert} for the alias child
     **/
    public SymbolAssert isAlias(String expectedAliasName) {
        isNotNull();
        isExactlyInstanceOf(AliasSymbol.class);
        AliasSymbol alias = (AliasSymbol) actual;
        assertThat(alias.alias())
            .as("alias")
            .isEqualTo(expectedAliasName);
        return new SymbolAssert(alias.symbol());
    }

    public SymbolAssert isAggregation(final String expectedName) {
        isNotNull();
        isExactlyInstanceOf(Aggregation.class);
        assertThat(((Aggregation) actual).signature().getName().name())
            .as("aggregation name")
            .isEqualTo(expectedName);
        return this;
    }

    @SafeVarargs
    public final SymbolAssert isAggregation(final String name, Consumer<Symbol>... argMatchers) {
        isAggregation(name);
        if (argMatchers != null) {
            assertThat(((Aggregation) actual).inputs()).satisfiesExactly(argMatchers);
        }
        return this;
    }

    public SymbolAssert isSQL(final String expectedStmt) {
        isNotNull();
        assertThat(SQLPrinter.print(actual)).isEqualTo(expectedStmt);
        return this;
    }

    public ObjectAssert<Object> extracting(String property) {
        return super.extracting(property, t -> assertThat(t));
    }
}
