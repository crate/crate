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

package io.crate.testing;

import io.crate.data.Input;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.types.DataType;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import javax.annotation.Nullable;
import java.util.List;
import java.util.ListIterator;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SymbolMatchers {

    public static Matcher<Symbol> isLiteral(Object expectedValue) {
        return isLiteral(expectedValue, null);
    }

    private static Matcher<Symbol> hasDataType(DataType type) {
        return withFeature(Symbol::valueType, "valueType", equalTo(type));
    }

    private static Matcher<Symbol> hasValue(Object expectedValue) {
        return withFeature(s -> ((Input) s).value(), "value", equalTo(expectedValue));
    }

    public static Matcher<Symbol> isLiteral(Object expectedValue, @Nullable final DataType type) {
        if (type == null) {
            return Matchers.allOf(Matchers.instanceOf(Literal.class), hasValue(expectedValue));
        }
        return Matchers.allOf(Matchers.instanceOf(Literal.class), hasValue(expectedValue), hasDataType(type));
    }

    public static Matcher<Symbol> isInputColumn(final Integer index) {
        return both(Matchers.<Symbol>instanceOf(InputColumn.class))
            .and(withFeature(s -> ((InputColumn) s).index(), "index", equalTo(index)));
    }

    public static Matcher<Symbol> isField(final String expectedName) {
        return isField(expectedName, null);
    }

    public static Matcher<Symbol> isField(final String expectedName, @Nullable final DataType dataType) {
        var hasExpectedName = withFeature(s -> ((Field) s).path().sqlFqn(), "name", equalTo(expectedName));
        if (dataType == null) {
            return both(Matchers.<Symbol>instanceOf(Field.class)).and(hasExpectedName);
        }
        return allOf(instanceOf(Field.class), hasExpectedName, hasDataType(dataType));
    }

    public static Matcher<Symbol> fieldPointsToReferenceOf(final String expectedName,
                                                           final String expectedRelationName) {
        java.util.function.Function<Symbol, Symbol> followFieldPointer = s -> {
            Symbol symbol = s;
            while (symbol instanceof Field) {
                symbol = ((Field) symbol).pointer();
            }
            return symbol;
        };
        return allOf(
            withFeature(followFieldPointer, "ref", isReference(expectedName)),
            withFeature(followFieldPointer
                            .andThen(s -> ((Reference) s).ident().tableIdent().fqn()),
                        "relationName",
                        equalTo(expectedRelationName))
        );
    }

    public static Matcher<Symbol> isFetchRef(int docIdIdx, String ref) {
        return isFetchRef(isInputColumn(docIdIdx), isReference(ref));
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
            Matchers.instanceOf(Reference.class),
            withFeature(s -> ((Reference) s).column(), "name", column),
            withFeature(s -> ((Reference) s).ident().tableIdent(), "relationName", relName),
            withFeature(Symbol::valueType, "valueType", type)
        );
    }

    private static <T> FeatureMatcher<Symbol, T> withFeature(java.util.function.Function<? super Symbol, T> getFeature,
                                                   String featureName,
                                                   Matcher<T> featureMatcher) {
        return new FeatureMatcher<>(featureMatcher, featureName, featureName) {

            @Override
            protected T featureValueOf(Symbol actual) {
                return getFeature.apply(actual);
            }
        };
    }

    public static Matcher<Symbol> isReference(final String expectedName, @Nullable final DataType dataType) {
        Matcher<Symbol> fm = withFeature(s -> ((Reference) s).column().sqlFqn(), "name", equalTo(expectedName));
        if (dataType == null) {
            return allOf(Matchers.instanceOf(Reference.class), fm);
        }
        return allOf(Matchers.instanceOf(Reference.class), hasDataType(dataType), fm);
    }

    @SafeVarargs
    public static Matcher<Symbol> isFunction(final String name, Matcher<? super Symbol>... argMatchers) {
        return both(isFunction(name))
            .and(withFeature(s -> ((Function) s).arguments(), "args", contains(argMatchers)));
    }

    public static Matcher<Symbol> isFunction(String name) {
        return both(Matchers.<Symbol>instanceOf(Function.class))
            .and(withFeature(s -> ((Function) s).info().ident().name(), "name", equalTo(name)));
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
            .and(withFeature(s -> ((Aggregation) s).functionIdent().name(), "name", equalTo(name)));
    }
}
