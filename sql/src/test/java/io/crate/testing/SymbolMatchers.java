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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Reference;
import io.crate.operation.Input;
import io.crate.sql.Identifiers;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.*;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

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
        return new TypeSafeDiagnosingMatcher<Symbol>() {

            @Override
            public boolean matchesSafely(Symbol item, Description desc) {
                if (!(item instanceof Field)) {
                    desc.appendText("not a Field: ").appendText(item.getClass().getName());
                    return false;
                }
                String name = ((Field) item).path().outputName();
                if (!name.equals(expectedName)) {
                    desc.appendText("different path ").appendValue(name);
                    return false;
                }
                if (dataType != null && !item.valueType().equals(dataType)) {
                    desc.appendText("different type ").appendValue(dataType.toString());
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                StringBuilder builder = new StringBuilder("a Field with path ").append(expectedName);
                if (dataType != null) {
                    builder.append(" and type").append(dataType.toString());
                }
                description.appendText(builder.toString());
            }
        };
    }

    public static Matcher<Symbol> isFetchRef(int docIdIdx, String ref) {
        return isFetchRef(isInputColumn(docIdIdx), isReference(ref));
    }

    private static Matcher<Symbol> isFetchRef(Matcher<Symbol> docIdMatcher, Matcher<Symbol> refMatcher) {

        FeatureMatcher<Symbol, Symbol> m1 = new FeatureMatcher<Symbol, Symbol>(
            docIdMatcher, "docId", "docId"
        ) {
            @Override
            protected Symbol featureValueOf(Symbol actual) {
                return ((FetchReference) actual).docId();
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
        return new TypeSafeDiagnosingMatcher<Symbol>() {

            @Override
            public boolean matchesSafely(Symbol item, Description desc) {
                if (!(item instanceof Reference)) {
                    desc.appendText("not a Reference: ").appendText(item.getClass().getName());
                    return false;
                }
                String name = ((Reference) item).ident().columnIdent().outputName();
                if (!name.equals(Identifiers.quoteIfNeeded(expectedName))) {
                    desc.appendText("different name ").appendValue(name);
                    return false;
                }
                if (dataType != null && !item.valueType().equals(dataType)) {
                    desc.appendText("different type ").appendValue(dataType.toString());
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                StringBuilder builder = new StringBuilder("a Reference with name ").append(expectedName);
                if (dataType != null) {
                    builder.append(" and type").append(dataType.toString());
                }
                description.appendText(builder.toString());
            }
        };
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
        return new TypeSafeDiagnosingMatcher<Symbol>() {
            @Override
            public boolean matchesSafely(Symbol item, Description mismatchDescription) {
                if (!(item instanceof Function)) {
                    mismatchDescription.appendText("not a Function: ").appendValue(item.getClass().getName());
                    return false;
                }
                FunctionIdent actualIdent = ((Function) item).info().ident();
                if (!actualIdent.name().equals(name)) {
                    mismatchDescription.appendText("wrong Function: ").appendValue(actualIdent.name());
                    return false;
                }
                if (argumentTypes != null) {
                    if (actualIdent.argumentTypes().size() != argumentTypes.size()) {
                        mismatchDescription.appendText("wrong number of arguments: ").appendValue(actualIdent.argumentTypes().size());
                        return false;
                    }

                    List<DataType> types = ((Function) item).info().ident().argumentTypes();
                    for (int i = 0, typesSize = types.size(); i < typesSize; i++) {
                        DataType type = types.get(i);
                        DataType expected = argumentTypes.get(i);
                        if (!expected.equals(type)) {
                            mismatchDescription.appendText("argument ").appendValue(
                                i + 1).appendText(" has wrong type ").appendValue(type.toString());
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is function ").appendText(name);
                if (argumentTypes != null) {
                    description.appendText(" with argument types: ");
                    for (DataType type : argumentTypes) {
                        description.appendText(type.toString()).appendText(" ");
                    }
                }
            }
        };
    }
}
