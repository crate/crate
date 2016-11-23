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

import io.crate.analyze.symbol.*;
import io.crate.metadata.Reference;
import io.crate.operation.Input;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import static org.hamcrest.Matchers.*;

public class SymbolMatchers {

    public static Matcher<Symbol> isLiteral(Object expectedValue) {
        return isLiteral(expectedValue, null);
    }

    private static Matcher<Symbol> hasDataType(DataType type) {
        return new FeatureMatcher<Symbol, DataType>(equalTo(type), "valueType", "valueType") {
            @Override
            protected DataType featureValueOf(Symbol actual) {
                return actual.valueType();
            }
        };
    }

    private static Matcher<Symbol> hasValue(Object expectedValue) {
        return new FeatureMatcher<Symbol, Object>(equalTo(expectedValue), "value", "value") {
            @Override
            protected Object featureValueOf(Symbol actual) {
                return ((Input) actual).value();
            }
        };
    }

    public static Matcher<Symbol> isLiteral(Object expectedValue, @Nullable final DataType type) {
        if (expectedValue instanceof String) {
            expectedValue = new BytesRef(((String) expectedValue));
        }
        if (type == null) {
            return Matchers.allOf(Matchers.instanceOf(Literal.class), hasValue(expectedValue));
        }
        return Matchers.allOf(Matchers.instanceOf(Literal.class), hasValue(expectedValue), hasDataType(type));
    }

    public static Matcher<Symbol> isInputColumn(final Integer index) {
        return both(Matchers.<Symbol>instanceOf(InputColumn.class)).and(
            new FeatureMatcher<Symbol, Integer>(equalTo(index), "index", "index") {
                @Override
                protected Integer featureValueOf(Symbol actual) {
                    return ((InputColumn) actual).index();
                }
            });
    }

    public static Matcher<Symbol> isField(final String expectedName) {
        return isField(expectedName, null);
    }

    public static Matcher<Symbol> isField(final String expectedName, @Nullable final DataType dataType) {
        FeatureMatcher<Symbol, String> fm = new FeatureMatcher<Symbol, String>(equalTo(expectedName), "path", "path") {
            @Override
            protected String featureValueOf(Symbol actual) {
                return ((Field) actual).path().outputName();
            }
        };
        if (dataType == null) {
            return fm;
        }
        return both(fm).and(hasDataType(dataType));
    }

    public static Matcher<Symbol> isFetchRef(int fetchIdIdx, String ref) {
        return isFetchRef(isInputColumn(fetchIdIdx), isReference(ref));
    }

    private static Matcher<Symbol> isFetchRef(Matcher<Symbol> fetchIdMatcher, Matcher<Symbol> refMatcher) {

        FeatureMatcher<Symbol, Symbol> m1 = new FeatureMatcher<Symbol, Symbol>(
            fetchIdMatcher, "fetchId", "fetchId"
        ) {
            @Override
            protected Symbol featureValueOf(Symbol actual) {
                return ((FetchReference) actual).fetchId();
            }
        };

        FeatureMatcher<Symbol, Symbol> m2 = new FeatureMatcher<Symbol, Symbol>(
            refMatcher, "ref", "ref"
        ) {
            @Override
            protected Symbol featureValueOf(Symbol actual) {
                return ((FetchReference) actual).ref();
            }
        };
        return allOf(Matchers.<Symbol>instanceOf(FetchReference.class), m1, m2);

    }

    public static Matcher<Symbol> isReference(String expectedName) {
        return isReference(expectedName, null);
    }

    public static Matcher<Symbol> isReference(final String expectedName, @Nullable final DataType dataType) {
        FeatureMatcher<Symbol, String> fm = new FeatureMatcher<Symbol, String>(equalTo(expectedName), "name", "name") {
            @Override
            protected String featureValueOf(Symbol actual) {
                return ((Reference) actual).ident().columnIdent().outputName();
            }
        };
        if (dataType == null) {
            return allOf(Matchers.<Symbol>instanceOf(Reference.class), fm);
        }
        return allOf(Matchers.<Symbol>instanceOf(Reference.class), hasDataType(dataType), fm);
    }

    @SafeVarargs
    public static Matcher<Symbol> isFunction(final String name, Matcher<Symbol>... argMatchers) {
        FeatureMatcher<Symbol, Collection<Symbol>> ma = new FeatureMatcher<Symbol, Collection<Symbol>>(
            contains(argMatchers), "args", "args") {
            @Override
            protected Collection<Symbol> featureValueOf(Symbol actual) {
                return ((Function) actual).arguments();
            }
        };
        return both(isFunction(name)).and(ma);
    }

    public static Matcher<Symbol> isFunction(String name) {
        FeatureMatcher<Symbol, String> mn = new FeatureMatcher<Symbol, String>(
            equalTo(name), "name", "name") {
            @Override
            protected String featureValueOf(Symbol actual) {
                return ((Function) actual).info().ident().name();
            }
        };
        return both(Matchers.<Symbol>instanceOf(Function.class)).and(mn);
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
        FeatureMatcher<Symbol, String> fm = new FeatureMatcher<Symbol, String>(equalTo(name), "name", "name") {
            @Override
            protected String featureValueOf(Symbol actual) {
                return ((Aggregation) actual).functionIdent().name();
            }
        };
        return both(Matchers.<Symbol>instanceOf(Aggregation.class)).and(fm);
    }
}
