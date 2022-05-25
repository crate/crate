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

import io.crate.data.Input;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.FetchStub;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.RelationName;
import io.crate.types.DataType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import javax.annotation.Nullable;
import java.util.List;
import java.util.ListIterator;

import static io.crate.testing.MoreMatchers.withFeature;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SymbolMatchers {

    public static Matcher<Symbol> isLiteral(Object expectedValue) {
        return isLiteral(expectedValue, null);
    }

    private static Matcher<Symbol> hasDataType(DataType<?> type) {
        return withFeature(Symbol::valueType, "valueType", equalTo(type));
    }

    private static Matcher<Symbol> hasValue(Object expectedValue) {
        return withFeature(s -> ((Input<?>) s).value(), "value", equalTo(expectedValue));
    }

    public static Matcher<Symbol> isLiteral(Object expectedValue, @Nullable final DataType<?> type) {
        if (type == null) {
            return Matchers.allOf(Matchers.instanceOf(Literal.class), hasValue(expectedValue));
        }
        return Matchers.allOf(Matchers.instanceOf(Literal.class), hasValue(expectedValue), hasDataType(type));
    }

    public static Matcher<Symbol> isInputColumn(final Integer index) {
        return both(Matchers.<Symbol>instanceOf(InputColumn.class))
            .and(withFeature(s -> ((InputColumn) s).index(), "index", equalTo(index)));
    }

    public static Matcher<Symbol> isFetchStub(String columnName) {
        return allOf(
            instanceOf(FetchStub.class),
            withFeature(x -> ((FetchStub) x).ref(), "", isReference(columnName))
        );
    }

    public static Matcher<Symbol> isField(final String expectedName) {
        return isField(expectedName, (DataType<?>) null);
    }

    public static Matcher<Symbol> isField(final String expectedName, RelationName relation) {
        return allOf(
            instanceOf(ScopedSymbol.class),
            withFeature(x -> ((ScopedSymbol) x).column().sqlFqn(), "", equalTo(expectedName)),
            withFeature(x -> ((ScopedSymbol) x).relation(), "", equalTo(relation))
        );
    }

    public static Matcher<Symbol> isFetchMarker(RelationName relation, Matcher<Iterable<? extends Symbol>> refsMatcher) {
        return allOf(
            instanceOf(FetchMarker.class),
            withFeature(x -> ((FetchMarker) x).relationName(), "", equalTo(relation)),
            withFeature(x -> ((FetchMarker) x).fetchRefs(), "", refsMatcher)
        );
    }

    public static Matcher<Symbol> isField(final String expectedName, @Nullable final DataType<?> dataType) {
        var hasExpectedName = withFeature(s -> ((ScopedSymbol) s).column().sqlFqn(), "", equalTo(expectedName));
        if (dataType == null) {
            return both(Matchers.<Symbol>instanceOf(ScopedSymbol.class)).and(hasExpectedName);
        }
        return allOf(instanceOf(ScopedSymbol.class), hasExpectedName, hasDataType(dataType));
    }

    public static Matcher<Symbol> isFetchRef(int docIdIdx, String ref) {
        return isFetchRef(isInputColumn(docIdIdx), isReference(ref));
    }

    public static Matcher<Symbol> isAlias(String alias, Matcher<Symbol> childMatcher) {
        return allOf(
            Matchers.instanceOf(AliasSymbol.class),
            withFeature(s -> ((AliasSymbol) s).alias(), "alias", equalTo(alias)),
            withFeature(s -> ((AliasSymbol) s).symbol(), "child", childMatcher)
        );
    }

    public static Matcher<Symbol> isFetchRef(Matcher<Symbol> fetchIdMatcher, Matcher<Symbol> refMatcher) {
        return allOf(
            Matchers.instanceOf(FetchReference.class),
            withFeature(s -> ((FetchReference) s).fetchId(), "fetchId", fetchIdMatcher),
            withFeature(s -> ((FetchReference) s).ref(), "ref", refMatcher)
        );
    }

    public static Matcher<Symbol> isReference(String expectedName) {
        return isReference(expectedName, null);
    }

    public static Matcher<Symbol> isReference(Matcher<ColumnIdent> column,
                                              Matcher<RelationName> relName,
                                              Matcher<DataType> type) {
        return allOf(
            Matchers.instanceOf(SimpleReference.class),
            withFeature(s -> ((SimpleReference) s).column(), "name", column),
            withFeature(s -> ((SimpleReference) s).ident().tableIdent(), "relationName", relName),
            withFeature(Symbol::valueType, "valueType", type)
        );
    }

    public static Matcher<Symbol> isReference(final String expectedName, @Nullable final DataType dataType) {
        Matcher<Symbol> fm = withFeature(s -> ((SimpleReference) s).column().sqlFqn(), "name", equalTo(expectedName));
        if (dataType == null) {
            return allOf(Matchers.instanceOf(SimpleReference.class), fm);
        }
        return allOf(Matchers.instanceOf(SimpleReference.class), hasDataType(dataType), fm);
    }

    public static Matcher<Symbol> isDynamicReference(String expectedName) {
        return allOf(
            Matchers.instanceOf(DynamicReference.class),
            isReference(expectedName)
        );
    }

    public static Matcher<Symbol> isVoidReference(String expectedName) {
        return allOf(
            Matchers.instanceOf(VoidReference.class),
            isReference(expectedName)
        );
    }

    @SafeVarargs
    public static Matcher<Symbol> isFunction(final String name, Matcher<? super Symbol>... argMatchers) {
        return both(isFunction(name))
            .and(withFeature(s -> ((Function) s).arguments(), "args", contains(argMatchers)));
    }

    public static Matcher<Symbol> isFunction(String name) {
        return both(Matchers.<Symbol>instanceOf(Function.class))
            .and(withFeature(s -> ((Function) s).name(), "name", equalTo(name)));
    }

    public static Matcher<Symbol> isFunction(final String name, @Nullable final List<DataType> argumentTypes) {
        if (argumentTypes == null) {
            return isFunction(name);
        }
        Matcher[] argMatchers = new Matcher[argumentTypes.size()];
        ListIterator<DataType> it = argumentTypes.listIterator();
        while (it.hasNext()) {
            int i = it.nextIndex();
            DataType type = it.next();
            argMatchers[i] = hasDataType(type);
        }
        //noinspection unchecked
        return isFunction(name, argMatchers);
    }

    public static Matcher<Symbol> isAggregation(String name) {
        return both(Matchers.<Symbol>instanceOf(Aggregation.class))
            .and(withFeature(s -> ((Aggregation) s).signature().getName().name(), "name", equalTo(name)));
    }

    @SafeVarargs
    public static Matcher<Symbol> isAggregation(final String name, Matcher<? super Symbol>... argMatchers) {
        return both(isAggregation(name))
            .and(withFeature(s -> ((Aggregation) s).inputs(), "args", contains(argMatchers)));
    }

    @SuppressWarnings("unchecked")
    public static Matcher<Symbol> isAggregation(final String name, @Nullable final List<DataType<?>> argumentTypes) {
        if (argumentTypes == null) {
            return isAggregation(name);
        }
        Matcher[] argMatchers = new Matcher[argumentTypes.size()];
        ListIterator<DataType<?>> it = argumentTypes.listIterator();
        while (it.hasNext()) {
            int i = it.nextIndex();
            DataType<?> type = it.next();
            argMatchers[i] = hasDataType(type);
        }
        return isAggregation(name, argMatchers);
    }
}
