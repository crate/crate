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

import static io.crate.execution.ddl.tables.MappingUtil.createMapping;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

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
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.Version;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.analyze.BoundCreateTable;
import io.crate.common.collections.Sorted;
import io.crate.data.Row;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.window.WindowFunctionModule;
import io.crate.expression.operator.OperatorModule;
import io.crate.expression.predicate.PredicateModule;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.tablefunctions.TableFunctionModule;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.settings.session.SessionSettingModule;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.user.User;

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
        StringBuilder sb = new StringBuilder();
        for (Object[] row : rows) {
            sb.append(printRow(row));
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printRow(Object[] row) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        boolean first = true;
        for (Object o : row) {
            first = printObject(out, first, o);
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
        } else if (o instanceof Object[] oArray) {
            out.print("[");
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
        return createNodeContext(List.of(User.CRATE_USER), additionalModules);
    }

    public static NodeContext createNodeContext(List<User> users, AbstractModule... additionalModules) {
        return new NodeContext(
            prepareModulesBuilder(additionalModules).createInjector().getInstance(Functions.class),
            () -> users
        );
    }

    private static ModulesBuilder prepareModulesBuilder(AbstractModule... additionalModules) {
        ModulesBuilder modulesBuilder = new ModulesBuilder()
            .add(new SessionSettingModule())
            .add(new OperatorModule())
            .add(new AggregationImplModule())
            .add(new ScalarFunctionModule())
            .add(new WindowFunctionModule())
            .add(new TableFunctionModule(Settings.EMPTY))
            .add(new PredicateModule());
        if (additionalModules != null) {
            for (AbstractModule module : additionalModules) {
                modulesBuilder.add(module);
            }
        }
        return modulesBuilder;
    }

    public static Reference createReference(String columnName, DataType<?> dataType) {
        return createReference("dummyTable", new ColumnIdent(columnName), dataType);
    }

    public static Reference createReference(ColumnIdent columnIdent, DataType<?> dataType) {
        return createReference("dummyTable", columnIdent, dataType);
    }

    public static Reference createReference(String tableName, ColumnIdent columnIdent, DataType<?> dataType) {
        return new SimpleReference(
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

    public static Reference refInfo(String fqColumnName,
                                    DataType<?> dataType,
                                    RowGranularity rowGranularity,
                                    String... nested) {
        String[] parts = fqColumnName.split("\\.");
        ReferenceIdent refIdent;

        List<String> nestedParts = null;
        if (nested.length > 0) {
            nestedParts = Arrays.asList(nested);
        }
        refIdent = switch (parts.length) {
            case 2 -> new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, parts[0]), parts[1], nestedParts);
            case 3 -> new ReferenceIdent(new RelationName(parts[0], parts[1]), parts[2], nestedParts);
            default -> throw new IllegalArgumentException(
                "fqColumnName must contain <table>.<column> or <schema>.<table>.<column>");
        };
        return new SimpleReference(refIdent, rowGranularity, dataType, 0, null);
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

    public static DataType<?> randomPrimitiveType() {
        return DataTypes.PRIMITIVE_TYPES.get(ThreadLocalRandom.current().nextInt(DataTypes.PRIMITIVE_TYPES.size()));
    }

    public static Map<String, Object> jsonMap(final String json) {
        try (XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            return parser.map();
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

    @SuppressWarnings("unchecked")
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
        int length = RandomizedTest.randomIntBetween(minLength, maxLength);
        var generator = DataTypeTesting.getDataGenerator(dataType);

        for (int i = 0; i < length; i++) {
            // 1/length chance
            if (RandomizedTest.randomIntBetween(0, length - 1) == 0) {
                values.add(null);
            } else {
                values.add(dataType.sanitizeValue(generator.get()));
            }
        }
        return values;
    }

    public static Map<String, Object> toMapping(BoundCreateTable boundCreateTable) {
        return toMapping(null, boundCreateTable);
    }

    public static Map<String, Object> toMapping(LongSupplier columnOidSupplier, BoundCreateTable boundCreateTable) {
        IntArrayList pKeysIndices = boundCreateTable.primaryKeysIndices();

        var policy = (String) boundCreateTable.tableParameter().mappings().get(ColumnPolicy.MAPPING_KEY);
        var tableColumnPolicy = policy != null ? ColumnPolicy.fromMappingValue(policy) : ColumnPolicy.STRICT;

        List<Reference> references;
        if (columnOidSupplier != null) {
            references = DocReferences.applyOid(
                    boundCreateTable.columns().values(),
                    columnOidSupplier
            );
        } else {
            references = new ArrayList<>(boundCreateTable.columns().values());
        }

        return createMapping(
            MappingUtil.AllocPosition.forNewTable(),
            boundCreateTable.pkConstraintName(),
            references,
            pKeysIndices,
            boundCreateTable.getCheckConstraints(),
            boundCreateTable.partitionedBy(),
            tableColumnPolicy,
            boundCreateTable.routingColumn().equals(DocSysColumns.ID) ? null : boundCreateTable.routingColumn().fqn()
        );

    }
}
