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
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.crate.Version;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.where.DocKeys;
import io.crate.core.collections.Sorted;
import io.crate.data.Bucket;
import io.crate.data.Buckets;
import io.crate.data.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.tablefunctions.TableFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

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
            out.print(MAP_JOINER.join(Sorted.sortRecursive((Map<String, Object>) o, true)));
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
            new ReferenceIdent(new TableIdent(Schemas.DOC_SCHEMA_NAME, tableName), columnIdent),
            RowGranularity.DOC,
            dataType);
    }

    public static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new BytesRef(encoded).utf8ToString();
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
                if (item.numColumns() != expected.size()) {
                    mismatchDescription.appendText("row size does not match: ")
                        .appendValue(item.numColumns()).appendText(" != ").appendValue(expected.size());
                    return false;
                }
                for (int i = 0; i < item.numColumns(); i++) {
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

    /**
     * Get the values at column index <code>index</code> within all <code>rows</code>
     */
    public static Object[] getColumn(Object[][] rows, int index) throws Exception {
        if (rows.length == 0 || rows[0].length <= index) {
            throw new NoSuchElementException("no column with index " + index);
        }
        Object[] column = new Object[rows.length];
        for (int i = 0; i < rows.length; i++) {
            column[i] = rows[i][index];
        }
        return column;
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
                refIdent = new ReferenceIdent(new TableIdent(Schemas.DOC_SCHEMA_NAME, parts[0]), parts[1], nestedParts);
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

    public static <T, K extends Comparable> Matcher<Iterable<? extends T>> isSortedBy(final com.google.common.base.Function<T, K> extractSortingKeyFunction) {
        return isSortedBy(extractSortingKeyFunction, false, null);
    }

    public static <T, K extends Comparable> Matcher<Iterable<? extends T>> isSortedBy(final com.google.common.base.Function<T, K> extractSortingKeyFunction,
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
                return (Comparable) input.get(sortingPos);
            }
        }, reverse, nullsFirst);
    }

    public static DataType randomPrimitiveType() {
        return DataTypes.PRIMITIVE_TYPES.get(ThreadLocalRandom.current().nextInt(DataTypes.PRIMITIVE_TYPES.size()));
    }

    public static Map<String, Object> jsonMap(String json) {
        try {
            return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, json).map();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert {@param s} into UTF8 encoded BytesRef with random offset and extra length
     * <p>
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

    /**
     * Converts file path separators of a string into canonical form
     * e.g. Windows: "/test/" --> "\test\"
     *      UNIX: "/test/"   --> "/test/"
     * @param str The string that contains file path separator
     * @return the resolved string
     */
    public static String resolveCanonicalString(String str) {
        return str.replaceAll("/", java.util.regex.Matcher.quoteReplacement(File.separator));
    }

    public static void assertCrateVersion(Object object, Version versionCreated, Version versionUpgraded) {
        assertThat((Map<String, String>) object,
            allOf(
                hasEntry(is(Version.Property.CREATED.toString()),
                    versionCreated == null ? nullValue() : is(Version.toStringMap(versionCreated))),
                hasEntry(is(Version.Property.UPGRADED.toString()),
                    versionUpgraded == null ? nullValue() : is(Version.toStringMap(versionUpgraded)))));
    }
}
