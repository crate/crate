/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.Lists;
import io.crate.metadata.*;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.scalar.SubstrFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestingHelpers {

    /**
     * prints the contents of a result array as a human readable table
     * @param result the data to be printed
     * @return a string representing a table
     */
    public static String printedTable(Object[][] result) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        for (Object[] row : result) {
            boolean first = true;
            for (Object o : row) {
                if (!first) {
                    out.print("| ");
                } else {
                    first = false;
                }
                if (o == null) {
                    out.print("NULL");
                } else if (o instanceof BytesRef) {
                    out.print(((BytesRef) o).utf8ToString());
                } else {
                    out.print(o.toString());
                }
            }
            out.println();
        }
        return os.toString();
    }

    public static Function createFunction(String functionName, DataType returnType, Symbol... arguments) {
        return createFunction(functionName, returnType, Arrays.asList(arguments));
    }

    public static Function createFunction(String functionName, DataType returnType, List<Symbol> arguments) {
        List<DataType> dataTypes = Lists.transform(arguments, new com.google.common.base.Function<Symbol, DataType>() {
            @Nullable
            @Override
            public DataType apply(@Nullable Symbol input) {
                assert input instanceof DataTypeSymbol;
                return ((DataTypeSymbol) input).valueType();
            }
        });
        return new Function(
                new FunctionInfo(new FunctionIdent(functionName, dataTypes), returnType), arguments);
    }

    public static Function eq(Symbol left, Symbol right) {
        return createFunction(EqOperator.NAME, DataTypes.BOOLEAN, Arrays.asList(left, right));
    }

    public static Function and(Symbol left, Symbol right) {
        return new Function(AndOperator.INFO,
                Arrays.asList(left, right)
        );
    }

    public static Function or(Symbol left, Symbol right) {
        return new Function(OrOperator.INFO, Arrays.asList(left, right));
    }

    public static Function not(Symbol negated) {
        return new Function(NotPredicate.INFO, Arrays.asList(negated));
    }

    public static Function substr(Symbol str, int n) {
        return createFunction(
                SubstrFunction.NAME,
                DataTypes.STRING,
                Arrays.asList(str, Literal.newLiteral(n)));
    }

    public static Reference createReference(String columnName, DataType dataType) {
        return createReference("dummyTable", new ColumnIdent(columnName), dataType);
    }

    public static Reference createReference(ColumnIdent columnIdent, DataType dataType) {
        return createReference("dummyTable", columnIdent, dataType);
    }

    public static Reference createReference(String tableName, ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceInfo(
                new ReferenceIdent(new TableIdent(null, tableName), columnIdent),
                RowGranularity.DOC,
                dataType
        ));
    }

    public static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new BytesRef(encoded).utf8ToString();
    }

    public static void assertLiteralSymbol(Symbol symbol, Map<String, Object> expectedValue) {
        assertLiteral(symbol, expectedValue, DataTypes.OBJECT);
    }

    public static void assertLiteralSymbol(Symbol symbol, Boolean expectedValue) {
        assertLiteral(symbol, expectedValue, DataTypes.BOOLEAN);
    }

    @SuppressWarnings("unchecked")
    public static void assertLiteralSymbol(Symbol symbol, String expectedValue) {
        assertThat(symbol, instanceOf(Literal.class));
        Object value = ((Literal)symbol).value();

        if (value instanceof String) {
            assertThat((String)value, is(expectedValue));
        } else {
            assertThat(((BytesRef) value).utf8ToString(), is(expectedValue));
        }
        assertEquals(DataTypes.STRING, ((Literal)symbol).valueType());
    }

    public static void assertLiteralSymbol(Symbol symbol, Long expectedValue) {
        assertLiteral(symbol, expectedValue, DataTypes.LONG);
    }

    public static void assertLiteralSymbol(Symbol symbol, Integer expectedValue) {
        assertLiteral(symbol, expectedValue, DataTypes.INTEGER);
    }

    public static void assertLiteralSymbol(Symbol symbol, Double expectedValue) {
        assertLiteral(symbol, expectedValue, DataTypes.DOUBLE);
    }

    public static void assertLiteralSymbol(Symbol symbol, Float expectedValue) {
        assertLiteral(symbol, expectedValue, DataTypes.FLOAT);
    }

    @SuppressWarnings("unchecked")
    private static <T> void assertLiteral(Symbol symbol, T expectedValue, DataType type) {
        assertThat(symbol, instanceOf(Literal.class));
        assertEquals(type, ((Literal)symbol).valueType());
        assertThat((T)((Literal) symbol).value(), is(expectedValue));
    }

    public static void assertNullLiteral(Symbol symbol) {
        assertLiteral(symbol, null, DataTypes.UNDEFINED);
    }

    public static void assertLiteralSymbol(Symbol symbol, Object expectedValue, DataType type) {
        assertThat(symbol, instanceOf(Literal.class));
        assertEquals(type, ((Literal)symbol).valueType());
        assertThat(((Literal) symbol).value(), is(expectedValue));
    }

    public static Matcher<Symbol> isLiteral(final Object expectedValue, final DataType type) {
        return new TypeSafeDiagnosingMatcher<Symbol>() {
            @Override
            protected boolean matchesSafely(Symbol item, Description mismatchDescription) {
                if (!(item instanceof Literal)) {
                    mismatchDescription.appendText("not a Literal: ").appendValue(item.getClass().getName());
                    return false;
                }
                if (!((Literal) item).valueType().equals(type)) {
                    mismatchDescription.appendText("wrong type ").appendValue(type.toString());
                }
                if (((Literal) item).value() == null && expectedValue != null) {
                    mismatchDescription.appendText("wrong value ").appendValue(((Literal) item).value());
                } else if (((Literal) item).value() == null && expectedValue == null) {
                    return true;
                }
                if (!((Literal) item).value().equals(expectedValue)) {
                    mismatchDescription.appendText("wrong value ").appendValue(((Literal) item).value());
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is Literal of type ")
                        .appendText(type.toString())
                        .appendText(" and value ").appendValue(expectedValue);
            }
        };
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
                String name = ((Reference) item).info().ident().columnIdent().fqn();
                if (!name.equals(expectedName)) {
                    desc.appendText("different name ").appendValue(name);
                    return false;
                }
                if (dataType != null && !((Reference) item).info().type().equals(dataType)) {
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

    public static Matcher<Symbol> isFunction(String name) {
        return isFunction(name, null);
    }

    public static Matcher<Symbol> isFunction(final String name, @Nullable final List<DataType> argumentTypes) {
        return new TypeSafeDiagnosingMatcher<Symbol>() {
            @Override
            public boolean matchesSafely(Symbol item, Description mismatchDescription) {
                if (!(item instanceof Function)) {
                    mismatchDescription.appendText("not a Function: ").appendValue(item.getClass().getName());
                    return false;
                }
                if (!((Function) item).info().ident().name().equals(name)) {
                    mismatchDescription.appendText("wrong Function: ").appendValue(name);
                    return false;
                }
                if (argumentTypes != null) {
                    if (((Function) item).info().ident().argumentTypes().size() != argumentTypes.size()) {
                        mismatchDescription.appendText("wrong number of arguments: ").appendValue(((Function) item).info().ident().argumentTypes().size());
                        return false;
                    }

                    List<DataType> types = ((Function) item).info().ident().argumentTypes();
                    for (int i = 0, typesSize = types.size(); i < typesSize; i++) {
                        DataType type = types.get(i);
                        DataType expected = argumentTypes.get(i);
                        if (!expected.equals(type)) {
                            mismatchDescription.appendText("argument ").appendValue(i + 1).appendText(" has wrong type ").appendValue(type.toString());
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
                    description.appendText("with argument types: ");
                    for (DataType type : argumentTypes) {
                        description.appendText(type.toString());
                    }
                }
            }
        };
    }

}
