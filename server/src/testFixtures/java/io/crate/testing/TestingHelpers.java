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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import java.util.stream.StreamSupport;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.TableParameters;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.ddl.tables.MappingUtil;
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
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.optimizer.LoadedRules;
import io.crate.role.Role;
import io.crate.statistics.TableStats;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

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

    @SuppressWarnings("unchecked")
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
        } else if (o instanceof List<?> list) {
            out.print("[");
            for (int i = 0; i < list.size(); i++) {
                printObject(out, true, list.get(i));
                if (i < list.size() - 1) {
                    out.print(", ");
                }
            }
            out.print("]");
        } else if (o.getClass().isArray()) {
            out.print("[");
            boolean arrayFirst = true;
            for (int i = 0, length = Array.getLength(o); i < length; i++) {
                if (!arrayFirst) {
                    out.print(", ");
                } else {
                    arrayFirst = false;
                }
                printObject(out, true, Array.get(o, i));
            }
            out.print("]");
        } else if (o instanceof Map) {
            out.print("{");
            Map<String, Object> map = sortRecursive((Map<String, Object>) o, true);
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
            out.print(o);
        }
        return first;
    }

    @SuppressWarnings("unchecked")
    private static LinkedHashMap<String, Object> sortRecursive(Map<String, Object> map, boolean sortOnlyMaps) {
        LinkedHashMap<String, Object> sortedMap = new LinkedHashMap<>(map.size(), 1.0f);
        ArrayList<String> sortedKeys = new ArrayList<>(map.keySet());
        Collections.sort(sortedKeys);
        for (String sortedKey : sortedKeys) {
            Object o = map.get(sortedKey);
            if (o instanceof Map) {
                //noinspection unchecked
                sortedMap.put(sortedKey, sortRecursive((Map<String, Object>) o, sortOnlyMaps));
            } else if (o instanceof Collection<?> collection) {
                sortedMap.put(sortedKey, sortOnlyMaps ? o : sortRecursive(collection));
            } else {
                sortedMap.put(sortedKey, o);
            }
        }
        return sortedMap;
    }

    @SuppressWarnings("unchecked")
    private static Collection<?> sortRecursive(Collection<?> collection) {
        if (collection.isEmpty()) {
            return collection;
        }
        Object firstElement = collection.iterator().next();
        if (firstElement instanceof Map) {
            ArrayList<Object> sortedList = new ArrayList<>(collection.size());
            for (Object obj : collection) {
                //noinspection unchecked
                sortedList.add(sortRecursive((Map<String, Object>) obj, true));
            }
            sortedList.sort(null);
            return sortedList;
        }

        ArrayList<Object> sortedList = new ArrayList<>(collection);
        sortedList.sort(null);
        return sortedList;
    }

    public static NodeContext createNodeContext() {
        return createNodeContext(null, List.of(Role.CRATE_USER));
    }

    public static NodeContext createNodeContext(List<Role> roles) {
        return createNodeContext(null, roles);
    }

    public static NodeContext createNodeContext(Schemas schemas, List<Role> roles) {
        return new NodeContext(
            Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE))),
            () -> roles,
            nodeContext -> schemas,
            new TableStats()
        );
    }

    public static Reference createReference(String columnName, DataType<?> dataType) {
        return createReference("dummyTable", ColumnIdent.of(columnName), dataType);
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
        assertThat(map)
            .hasEntrySatisfying(
                Version.Property.CREATED.toString(),
                s -> {
                    if (versionCreated == null) {
                        assertThat(s).isNull();
                    } else {
                        assertThat(s).isEqualTo(versionCreated.externalNumber());
                    }
                })
            .hasEntrySatisfying(
                Version.Property.UPGRADED.toString(),
                s -> {
                    if (versionUpgraded == null) {
                        assertThat(s).isNull();
                    } else {
                        assertThat(s).isEqualTo(versionUpgraded.externalNumber());
                    }
                });
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

        var tableColumnPolicy = TableParameters.COLUMN_POLICY.get(boundCreateTable.settings());
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
            Lists.map(boundCreateTable.primaryKeys(), Reference::column),
            boundCreateTable.getCheckConstraints(),
            Lists.map(boundCreateTable.partitionedBy(), Reference::column),
            tableColumnPolicy,
            boundCreateTable.routingColumn()
        );
    }
}
