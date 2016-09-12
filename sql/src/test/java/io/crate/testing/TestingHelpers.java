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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.crate.analyze.symbol.*;
import io.crate.analyze.where.DocKeys;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.core.collections.Sorted;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.tablefunctions.TableFunctionModule;
import io.crate.sql.Identifiers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.*;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

public class TestingHelpers {

    /**
     * prints the contents of a result array as a human readable table
     *
     * @param result the data to be printed
     * @return a string representing a table
     */
    public static String printedTable(Object[][] result) {
        return printRows(Arrays.asList(result));
    }

    public static String printedTable(Bucket result) {
        return printRows(Arrays.asList(Buckets.materialize(result)));
    }

    public static String printRows(Iterable<Object[]> rows) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        for (Object[] row : rows) {
            boolean first = true;
            for (Object o : row) {
                first = printObject(out, first, o);
            }
            out.print("\n");
        }
        return os.toString();
    }

    private static boolean printObject(PrintStream out, boolean first, Object o) {
        if (!first) {
            out.print("| ");
        } else {
            first = false;
        }
        if (o == null) {
            out.print("NULL");
        } else if (o instanceof BytesRef) {
            out.print(((BytesRef) o).utf8ToString());
        } else if (o instanceof Object[]) {
            out.print("[");
            Object[] oArray = (Object[]) o;
            for (int i = 0; i < oArray.length; i++) {
                printObject(out, true, oArray[i]);
                if (i < oArray.length - 1) {
                    out.print(", ");
                }
            }
            out.print("]");
        } else if (o.getClass().isArray()) {
            out.print("[");
            boolean arrayFirst = true;
            for (int i = 0, length = Array.getLength(o); i < length; i++) {
                if (!arrayFirst) {
                    out.print(",v");
                } else {
                    arrayFirst = false;
                }
                printObject(out, first, Array.get(o, i));
            }
            out.print("]");
        } else if (o instanceof Map) {
            out.print("{");
            out.print(MAP_JOINER.join(Sorted.sortRecursive((Map<String, Object>)o, true)));
            out.print("}");
        } else {
            out.print(o.toString());
        }
        return first;
    }

    private final static Joiner.MapJoiner MAP_JOINER = Joiner.on(", ").useForNull("null").withKeyValueSeparator("=");

    public static String mapToSortedString(Map<String, Object> map) {
        return MAP_JOINER.join(Sorted.sortRecursive(map));
    }

    public static Functions getFunctions() {
        return new ModulesBuilder()
                .add(new AggregationImplModule())
                .add(new PredicateModule())
                .add(new TableFunctionModule())
                .add(new ScalarFunctionModule())
                .add(new OperatorModule()).createInjector().getInstance(Functions.class);
    }

    public static Reference createReference(String columnName, DataType dataType) {
        return createReference("dummyTable", new ColumnIdent(columnName), dataType);
    }

    public static Reference createReference(ColumnIdent columnIdent, DataType dataType) {
        return createReference("dummyTable", columnIdent, dataType);
    }

    public static Reference createReference(String tableName, ColumnIdent columnIdent, DataType dataType) {
        return new Reference(
                new ReferenceIdent(new TableIdent(null, tableName), columnIdent),
                RowGranularity.DOC,
                dataType);
    }

    public static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new BytesRef(encoded).utf8ToString();
    }

    public static Matcher<Symbol> isLiteral(Object expectedValue) {
        return isLiteral(expectedValue, null);
    }

    public static Matcher<Symbol> hasDataType(DataType type) {
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

    private static final com.google.common.base.Function<Object, Object> bytesRefToString =
            new com.google.common.base.Function<Object, Object>() {

                @Nullable
                @Override
                public Object apply(@Nullable Object input) {
                    if (input instanceof BytesRef) {
                        return ((BytesRef) input).utf8ToString();
                    }
                    return input;
                }
            };

    public static Matcher<Row> isNullRow() {
        return isRow((Object) null);
    }

    public static Matcher<Row> isRow(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        final List<Object> expected = Lists.transform(Arrays.asList(cells), bytesRefToString);
        return new TypeSafeDiagnosingMatcher<Row>() {
            @Override
            protected boolean matchesSafely(Row item, Description mismatchDescription) {
                if (item.size() != expected.size()) {
                    mismatchDescription.appendText("row size does not match: ")
                            .appendValue(item.size()).appendText(" != ").appendValue(expected.size());
                    return false;
                }
                for (int i = 0; i < item.size(); i++) {
                    Object actual = bytesRefToString.apply(item.get(i));
                    if (!Objects.equals(expected.get(i), actual)) {
                        mismatchDescription.appendText("value at pos ")
                                .appendValue(i)
                                .appendText(" does not match: ")
                                .appendValue(expected.get(i))
                                .appendText(" != ")
                                .appendValue(actual);
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is Row with cells: ")
                        .appendValue(expected);
            }
        };
    }

    public static Matcher<DocKeys.DocKey> isNullDocKey() {
        return isDocKey(new Object[]{null});
    }

    public static Matcher<DocKeys.DocKey> isDocKey(Object... keys) {
        final List<Object> expected = Arrays.asList(keys);
        return new TypeSafeDiagnosingMatcher<DocKeys.DocKey>() {
            @Override
            protected boolean matchesSafely(DocKeys.DocKey item, Description mismatchDescription) {
                List objects = Lists.transform(
                        Lists.transform(item.values(), ValueSymbolVisitor.VALUE.function), bytesRefToString);
                if (!expected.equals(objects)) {
                    mismatchDescription.appendText("is DocKey with values: ").appendValue(objects);
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is DocKey with values: ")
                        .appendValue(expected);
            }
        };
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
                if (dataType != null && !((Field) item).valueType().equals(dataType)) {
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

    public static Matcher<Symbol> isFetchRef(Matcher<Symbol> docIdMatcher, Matcher<Symbol> refMatcher) {

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

    /**
     * Get the values at column index <code>index</code> within all <code>rows</code>
     */
    public static
    @Nullable
    Object[] getColumn(Object[][] rows, int index) throws Exception {
        if (rows.length == 0 || rows[0].length <= index) {
            throw new NoSuchElementException("no column with index " + index);
        }
        Object[] column = new Object[rows.length];
        for (int i = 0; i < rows.length; i++) {
            column[i] = rows[i][index];
        }
        return column;
    }

    public static ThreadPool newMockedThreadPool() {
        ThreadPool threadPool = Mockito.mock(ThreadPool.class);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                executorService.shutdown();
                return null;
            }
        }).when(threadPool).shutdown();
        when(threadPool.executor(anyString())).thenReturn(executorService);

        try {
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    executorService.awaitTermination(1, TimeUnit.SECONDS);
                    return null;
                }
            }).when(threadPool).awaitTermination(anyLong(), any(TimeUnit.class));
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }

        return threadPool;
    }

    public static Reference refInfo(String fqColumnName, DataType dataType, RowGranularity rowGranularity, String... nested) {
        String[] parts = fqColumnName.split("\\.");
        ReferenceIdent refIdent;

        List<String> nestedParts = null;
        if (nested.length > 0) {
            nestedParts = Arrays.asList(nested);
        }
        switch (parts.length) {
            case 2:
                refIdent = new ReferenceIdent(new TableIdent(null, parts[0]), parts[1], nestedParts);
                break;
            case 3:
                refIdent = new ReferenceIdent(new TableIdent(parts[0], parts[1]), parts[2], nestedParts);
                break;
            default:
                throw new IllegalArgumentException("fqColumnName must contain <table>.<column> or <schema>.<table>.<column>");
        }
        return new Reference(refIdent, rowGranularity, dataType);
    }

    public static <T> Matcher<T> isSQL(final String stmt) {
        return new BaseMatcher<T>() {
            @Override
            public boolean matches(Object item) {
                return SQLPrinter.print(item).equals(stmt);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(stmt);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText(SQLPrinter.print(item));
            }
        };
    }

    public static Object[][] range(int from, int to) {
        int size = to - from;
        Object[][] result = new Object[to - from][];
        for (int i = 0; i < size; i++) {
            result[i] = new Object[]{i + from};
        }
        return result;
    }

    public static <T, K extends Comparable> Matcher<Iterable<? extends T>> isSortedBy(final com.google.common.base.Function<T,K> extractSortingKeyFunction) {
        return isSortedBy(extractSortingKeyFunction, false, null);
    }

    public static <T, K extends Comparable> Matcher<Iterable<? extends T>> isSortedBy(final com.google.common.base.Function<T,K> extractSortingKeyFunction,
                                                                            final boolean descending,
                                                                            @Nullable final Boolean nullsFirst) {
        Ordering<K> ordering = Ordering.natural();
        if (descending) {
            ordering = ordering.reverse();
        }
        if (nullsFirst != null && nullsFirst) {
            ordering = ordering.nullsFirst();
        } else {
            ordering = ordering.nullsLast();
        }
        final Ordering<K> ord = ordering;

        return new TypeSafeDiagnosingMatcher<Iterable<? extends T>>() {
            @Override
            protected boolean matchesSafely(Iterable<? extends T> item, Description mismatchDescription) {
                K previous = null;
                int i = 0;
                for (T elem : item) {
                    K current = extractSortingKeyFunction.apply(elem);
                    if (previous != null) {
                        if (ord.compare(previous, current) > 0) {
                            mismatchDescription
                                    .appendText("element ").appendValue(current)
                                    .appendText(" at position ").appendValue(i)
                                    .appendText(" is ")
                                    .appendText(descending ? "bigger" : "smaller")
                                    .appendText(" than previous element ")
                                    .appendValue(previous);
                            return false;
                        }
                    }
                    i++;
                    previous = current;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("expected iterable to be sorted ");
                if (descending) {
                    description.appendText("in DESCENDING order");
                } else {
                    description.appendText("in ASCENDING order");
                }
            }
        };
    }

    public static Matcher<Iterable<? extends Row>> hasSortedRows(final int sortingPos, final boolean reverse, @Nullable final Boolean nullsFirst) {
        return TestingHelpers.isSortedBy(new com.google.common.base.Function<Row, Comparable>() {
            @Nullable
            @Override
            public Comparable apply(@Nullable Row input) {
                assert input != null;
                return (Comparable)input.get(sortingPos);
            }
        }, reverse, nullsFirst);
    }

    public static DataType randomPrimitiveType() {
        return DataTypes.PRIMITIVE_TYPES.get(ThreadLocalRandom.current().nextInt(DataTypes.PRIMITIVE_TYPES.size()));
    }

    public static Map<String, Object> jsonMap(String json) {
        try {
            return JsonXContent.jsonXContent.createParser(json).map();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert {@param s} into UTF8 encoded BytesRef with random offset and extra length
     *
     * This should be preferred over `new BytesRef` in tests to make sure that implementations using BytesRef
     * handle offset and length correctly (use {@link BytesRef#length} instead of {@link BytesRef#bytes#length}
     */
    public static BytesRef bytesRef(String s, Random random) {
        byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
        int extraLength = random.nextInt(100);
        int offset = 0;
        if (extraLength > 0) {
            offset = random.nextInt(extraLength);
        }
        byte[] buffer = new byte[strBytes.length + extraLength];
        random.nextBytes(buffer);
        System.arraycopy(strBytes, 0, buffer, offset, strBytes.length);
        return new BytesRef(buffer, offset, strBytes.length);
    }
}
