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
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.*;
import io.crate.analyze.where.DocKeys;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
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
                    out.print(Arrays.deepToString((Object[]) o));
                } else if (o.getClass().isArray()) {
                    out.print("[");
                    boolean arrayFirst = true;
                    for (int i = 0, length = Array.getLength(o); i < length; i++) {
                        if (!arrayFirst) {
                            out.print(",v");
                        } else {
                            arrayFirst = false;
                        }
                        out.print(Array.get(o, i));

                    }
                    out.print("]");
                } else if (o instanceof Map) {
                    out.print("{");
                    out.print(MAP_JOINER.join(sortMapByKeyRecursive((Map<String, Object>)o, true)));
                    out.print("}");
                } else {
                    out.print(o.toString());
                }
            }
            out.print("\n");
        }
        return os.toString();
    }

    private final static Joiner.MapJoiner MAP_JOINER = Joiner.on(", ").useForNull("null").withKeyValueSeparator("=");

    private static LinkedHashMap<String, Object> sortMapByKeyRecursive(Map<String, Object> map) {
        return sortMapByKeyRecursive(map, false);
    }

    private static LinkedHashMap<String, Object> sortMapByKeyRecursive(Map<String, Object> map, boolean sortOnlyMaps) {
        LinkedHashMap<String, Object> sortedMap = new LinkedHashMap<>(map.size(), 1.0f);
        ArrayList<String> sortedKeys = Lists.newArrayList(map.keySet());
        Collections.sort(sortedKeys);
        for (String sortedKey : sortedKeys) {
            Object o = map.get(sortedKey);
            if (o instanceof Map) {
                //noinspection unchecked
                sortedMap.put(sortedKey, sortMapByKeyRecursive((Map<String, Object>) o, sortOnlyMaps));
            } else if (o instanceof Collection) {
                sortedMap.put(sortedKey, sortOnlyMaps ? o : sortCollectionRecursive((Collection) o));
            } else {
                sortedMap.put(sortedKey, o);
            }
        }
        return sortedMap;
    }

    private static Collection sortCollectionRecursive(Collection collection) {
        if (collection.size() == 0) {
            return collection;
        }
        Object firstElement = collection.iterator().next();
        if (firstElement instanceof Map) {
            ArrayList sortedList = new ArrayList(collection.size());
            for (Object obj : collection) {
                //noinspection unchecked
                sortedList.add(sortMapByKeyRecursive((Map<String, Object>) obj, true));
            }
            Collections.sort(sortedList);
            return sortedList;
        }

        ArrayList sortedList = Lists.newArrayList(collection);
        Collections.sort(sortedList);
        return sortedList;
    }

    public static String mapToSortedString(Map<String, Object> map) {
        return MAP_JOINER.join(sortMapByKeyRecursive(map));
    }

    /**
     * @deprecated use {@link SqlExpressions} instead
     */
    @Deprecated
    public static Function createFunction(String functionName, DataType returnType, Symbol... arguments) {
        return createFunction(functionName, returnType, Arrays.asList(arguments), true);
    }

    /**
     * @deprecated use {@link SqlExpressions} instead
     */
    @Deprecated
    public static Function createFunction(String functionName, DataType returnType, List<Symbol> arguments) {
        return createFunction(functionName, returnType, arguments, true);
    }

    /**
     * @deprecated use {@link SqlExpressions} instead
     */
    @Deprecated
    public static Function createFunction(String functionName, DataType returnType, List<Symbol> arguments, boolean deterministic) {
        List<DataType> dataTypes = Symbols.extractTypes(arguments);
        return new Function(
                new FunctionInfo(new FunctionIdent(functionName, dataTypes), returnType, FunctionInfo.Type.SCALAR, deterministic),
                arguments
        );
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

    private static final com.google.common.base.Function<Object, Object> stringToBytesRef =
            new com.google.common.base.Function<Object, Object>() {

                @Nullable
                @Override
                public Object apply(@Nullable Object input) {
                    if (input instanceof String) {
                        return new BytesRef(input.toString());
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

    public static Matcher<Symbol> isField(final Integer index) {
        return both(Matchers.<Symbol>instanceOf(Field.class)).and(
                new FeatureMatcher<Symbol, Integer>(equalTo(index), "index", "index") {
                    @Override
                    protected Integer featureValueOf(Symbol actual) {
                        return ((Field) actual).index();
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
        return isFetchRef(isField(docIdIdx), isReference(ref));
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
                String name = ((Reference) item).info().ident().columnIdent().outputName();
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

    /**
     * @deprecated use {@link SqlExpressions} instead
     */
    @Deprecated
    public static WhereClause whereClause(String opname, Symbol left, Symbol right) {
        return new WhereClause(new Function(new FunctionInfo(
                new FunctionIdent(opname, Arrays.asList(left.valueType(), right.valueType())), DataTypes.BOOLEAN),
                Arrays.asList(left, right)
        ));
    }

    public static ThreadPool newMockedThreadPool() {
        ThreadPool threadPool = PowerMockito.mock(ThreadPool.class);
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

    public static ReferenceInfo refInfo(String fqColumnName, DataType dataType, RowGranularity rowGranularity, String... nested) {
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
        return new ReferenceInfo(refIdent, rowGranularity, dataType);
    }

    public static Matcher<? super Object> isSQL(final String stmt) {
        return new BaseMatcher<Object>() {
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

    private static class CauseMatcher extends TypeSafeMatcher<Throwable> {

        private final Class<? extends Throwable> type;
        private final String expectedMessage;

        public CauseMatcher(Class<? extends Throwable> type, @Nullable String expectedMessage) {
            this.type = type;
            this.expectedMessage = expectedMessage;
        }

        @Override
        protected boolean matchesSafely(Throwable item) {
            return item.getClass().isAssignableFrom(type)
                   && (null == expectedMessage || item.getMessage().contains(expectedMessage));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("expects type ").appendValue(type);
            if (expectedMessage != null) {
                description.appendText(" and a message ").appendValue(expectedMessage);
            }
        }
    }

    public static Matcher<Throwable> cause(Class<? extends Throwable> type) {
        return cause(type, null);
    }

    public static Matcher<Throwable> cause(Class<? extends Throwable> type, String expectedMessage) {
        return new CauseMatcher(type, expectedMessage);
    }

    public static Object[][] range(int from, int to) {
        int size = to - from;
        Object[][] result = new Object[to - from][];
        for (int i = 0; i < size; i++) {
            result[i] = new Object[]{i + from};
        }
        return result;
    }

    public static Matcher<Bucket> isSorted(final int sortingPos, final boolean reverse, @Nullable final Boolean nullsFirst) {
        Ordering ordering = Ordering.natural();
        if (reverse) {
            ordering = ordering.reverse();
        }
        if (nullsFirst != null && nullsFirst) {
            ordering = ordering.nullsFirst();
        } else {
            ordering = ordering.nullsLast();
        }
        final Ordering ord = ordering;
        return new TypeSafeDiagnosingMatcher<Bucket>() {


            @Override
            protected boolean matchesSafely(Bucket item, Description mismatchDescription) {
                Object previous = null;
                int i = 0;
                for (Row row : item) {
                    Object current = row.get(sortingPos);
                    if (previous != null) {
                        if (ord.compare(previous, current) > 0) {
                            mismatchDescription
                                    .appendText("element ").appendValue(current)
                                    .appendText(" at position ").appendValue(i)
                                    .appendText(" is ")
                                    .appendText(reverse ? "bigger" : "smaller")
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
                description.appendText("expected bucket to be sorted by position: ")
                        .appendValue(sortingPos);
                if (reverse) {
                    description.appendText(" reverse ");
                }
                description.appendText(" nulls ").appendText(nullsFirst != null && nullsFirst ? "first" : "last");
            }
        };
    }

    public static DataType randomPrimitiveType() {
        return DataTypes.PRIMITIVE_TYPES.get(ThreadLocalRandom.current().nextInt(DataTypes.PRIMITIVE_TYPES.size()));
    }
}
