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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.analyze.where.DocKeys;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Sorted;
import io.crate.data.Row;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.window.WindowFunctionModule;
import io.crate.expression.operator.OperatorModule;
import io.crate.expression.predicate.PredicateModule;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Literal;
import io.crate.expression.tablefunctions.TableFunctionModule;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.session.SessionSettingModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
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

    public static String printedTable(Iterable<Row> result) {
        return printRows(StreamSupport.stream(result.spliterator(), false)
            .map(Row::materialize)
            ::iterator
        );
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
            //noinspection unchecked
            Map<String, Object> map = Sorted.sortRecursive((Map<String, Object>) o, true);
            Iterator<String> it = map.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                out.print(key + "=");
                printObject(out, true, map.get(key));
                if (it.hasNext()) {
                    out.print(", ");
                }
            }
            out.print("}");
        } else {
            out.print(o.toString());
        }
        return first;
    }

    public static String mapToSortedString(Map<String, Object> map) {
        return Sorted.sortRecursive(map).entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(", "));
    }

    public static NodeContext createNodeContext(AbstractModule... additionalModules) {
        ModulesBuilder modulesBuilder = new ModulesBuilder()
            .add(new SessionSettingModule())
            .add(new OperatorModule())
            .add(new AggregationImplModule())
            .add(new ScalarFunctionModule())
            .add(new WindowFunctionModule())
            .add(new TableFunctionModule())
            .add(new PredicateModule());
        if (additionalModules != null) {
            for (AbstractModule module : additionalModules) {
                modulesBuilder.add(module);
            }
        }
        return new NodeContext(modulesBuilder.createInjector().getInstance(Functions.class));
    }

    public static Reference createReference(String columnName, DataType dataType) {
        return createReference("dummyTable", new ColumnIdent(columnName), dataType);
    }

    public static Reference createReference(ColumnIdent columnIdent, DataType dataType) {
        return createReference("dummyTable", columnIdent, dataType);
    }

    public static Reference createReference(String tableName, ColumnIdent columnIdent, DataType dataType) {
        return new Reference(
            new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, tableName), columnIdent),
            RowGranularity.DOC,
            dataType,
            0,
            null
        );
    }

    public static String readFile(String path) throws IOException {
        return String.join("\n", Files.readAllLines(Paths.get(path)));
    }


    public static Matcher<Row> isNullRow() {
        return isRow((Object) null);
    }

    public static Matcher<Row> isRow(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        final List<Object> expected = Arrays.asList(cells);
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(Row item, Description mismatchDescription) {
                if (item.numColumns() != expected.size()) {
                    mismatchDescription.appendText("row size does not match: ")
                        .appendValue(item.numColumns()).appendText(" != ").appendValue(expected.size());
                    return false;
                }
                for (int i = 0; i < item.numColumns(); i++) {
                    Object actual = item.get(i);
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

    public static Matcher<DocKeys.DocKey> isDocKey(Object... keys) {
        final List<Object> expected = Arrays.asList(keys);
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(DocKeys.DocKey item, Description mismatchDescription) {
                List<Object> docKeyValues = Lists2.map(item.values(), s -> ((Literal) s).value());
                if (!expected.equals(docKeyValues)) {
                    mismatchDescription.appendText("is DocKey with values: ").appendValue(docKeyValues);
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
                refIdent = new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, parts[0]), parts[1], nestedParts);
                break;
            case 3:
                refIdent = new ReferenceIdent(new RelationName(parts[0], parts[1]), parts[2], nestedParts);
                break;
            default:
                throw new IllegalArgumentException("fqColumnName must contain <table>.<column> or <schema>.<table>.<column>");
        }
        return new Reference(refIdent, rowGranularity, dataType, 0, null);
    }

    public static <T> Matcher<T> isSQL(final String stmt) {
        return new BaseMatcher<>() {
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

    public static Matcher<SQLResponse> isPrintedTable(String expectedPrintedResponse) {
        return new FeatureMatcher<>(equalTo(expectedPrintedResponse), "same output", "printedTable") {
            @Override
            protected String featureValueOf(SQLResponse actual) {
                return printedTable(actual.rows());
            }
        };
    }

    public static <T, K extends Comparable> Matcher<Iterable<? extends T>> isSortedBy(final Function<T, K> extractSortingKeyFunction) {
        return isSortedBy(extractSortingKeyFunction, false, null);
    }

    public static <T, K extends Comparable> Matcher<Iterable<? extends T>> isSortedBy(
        final Function<T, K> extractSortingKeyFunction,
        final boolean descending,
        @Nullable final Boolean nullsFirst) {
        Comparator<K> comparator = Comparator.naturalOrder();
        if (descending) {
            comparator = Comparator.reverseOrder();
        }
        if (nullsFirst != null && nullsFirst) {
            comparator = Comparator.nullsFirst(comparator);
        } else {
            comparator = Comparator.nullsLast(comparator);
        }
        Comparator<K> finalComparator = comparator;
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(Iterable<? extends T> item, Description mismatchDescription) {
                K previous = null;
                int i = 0;
                for (T elem : item) {
                    K current = extractSortingKeyFunction.apply(elem);
                    if (previous != null) {
                        if (finalComparator.compare(previous, current) > 0) {
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
        return TestingHelpers.isSortedBy(new Function<>() {
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
            return JsonXContent.JSON_XCONTENT.createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json).map();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        var map = (Map<String, String>) object;
        assertThat(
            map,
            allOf(
                hasEntry(
                    is(Version.Property.CREATED.toString()),
                    versionCreated == null ? nullValue() : is(versionCreated.externalNumber())),
                hasEntry(
                    is(Version.Property.UPGRADED.toString()),
                    versionUpgraded == null ? nullValue() : is(versionUpgraded.externalNumber()))));
    }

    public static <T> List<T> getRandomsOfType(int minLength, int maxLength, DataType<T> dataType) {
        var values = new ArrayList<T>();
        int length  = RandomizedTest.randomIntBetween(minLength, maxLength);
        var generator = DataTypeTesting.getDataGenerator(dataType);

        for (int i = 0; i < length; i++) {
            // 1/length chance
            if (RandomizedTest.randomIntBetween(0, length-1) == 0) {
                values.add(null);
            } else {
                values.add(dataType.sanitizeValue(generator.get()));
            }
        }
        return values;
    }
}
