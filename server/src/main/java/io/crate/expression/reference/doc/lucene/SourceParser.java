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

package io.crate.expression.reference.doc.lucene;

import static org.elasticsearch.common.xcontent.XContentParser.Token.START_ARRAY;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NULL;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.server.xcontent.XContentHelper;
import io.crate.sql.tree.BitString;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.FloatVectorType;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;
import io.crate.types.UndefinedType;

public final class SourceParser {

    public static final String UNKNOWN_COLUMN_PREFIX = "_u_";
    private final Map<String, Object> requiredColumns = new HashMap<>();
    private final Set<String> droppedColumns;
    private final UnaryOperator<String> lookupNameBySourceKey;

    public SourceParser(Set<Reference> droppedColumns, UnaryOperator<String> lookupNameBySourceKey) {
        // Use a Set of string fqn instead of ColumnIdent to avoid creating ColumnIdent objects to call `contains`
        this.droppedColumns = droppedColumns.stream().map(r -> r.column().fqn()).collect(Collectors.toUnmodifiableSet());
        this.lookupNameBySourceKey = lookupNameBySourceKey;
    }

    public SourceParser(DocTableInfo table) {
        this.droppedColumns
            = table.droppedColumns().stream().map(r -> r.column().fqn()).collect(Collectors.toUnmodifiableSet());
        this.lookupNameBySourceKey = table.lookupNameBySourceKey();
        register(table.columns());
    }

    public void register(List<? extends Symbol> symbols) {
        if (!Symbols.hasColumn(symbols, DocSysColumns.DOC)) {
            Consumer<Reference> register = ref -> {
                if (ref.column().isSystemColumn() == false && ref.granularity() == RowGranularity.DOC) {
                    register(DocReferences.toSourceLookup(ref).column(), ref.valueType());
                }
            };
            for (Symbol symbol : symbols) {
                RefVisitor.visitRefs(symbol, register);
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    public void register(ColumnIdent docColumn, DataType<?> type) {
        assert docColumn.name().equals(DocSysColumns.DOC.name()) && docColumn.path().size() > 0
            : "All columns registered for sourceParser must start with _doc";

        List<String> path = docColumn.path();
        if (path.size() == 1) {
            requiredColumns.put(docColumn.path().get(0), type);
        } else {
            Map<String, Object> columns = requiredColumns;
            for (int i = 0; i < path.size(); i++) {
                String part = path.get(i);
                if (i + 1 == path.size()) {
                    columns.put(part, type);
                } else {
                    Object object = columns.get(part);
                    if (object instanceof Map map) {
                        columns = map;
                    } else if (object instanceof DataType) {
                        break;
                    } else {
                        HashMap<String, Object> children = new HashMap<>();
                        columns.put(part, children);
                        columns = children;
                    }
                }
            }
        }
    }

    public Map<String, Object> parse(BytesReference bytes, boolean includeUnknownCols) {
        try (InputStream inputStream = XContentHelper.getUncompressedInputStream(bytes);
             XContentParser parser = XContentType.JSON.xContent().createParser(
                 NamedXContentRegistry.EMPTY,
                 DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                 inputStream
             )) {
            Token token = parser.currentToken();
            if (token == null) {
                parser.nextToken();
            }
            return parseObject(
                parser,
                requiredColumns,
                droppedColumns,
                lookupNameBySourceKey,
                new StringBuilder(),
                includeUnknownCols
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, Object> parse(BytesReference bytes) {
        return parse(bytes, false);
    }

    private static Object parseArray(XContentParser parser,
                                     @Nullable DataType<?> type,
                                     @Nullable Map<String, Object> requiredColumns,
                                     Set<String> droppedColumns,
                                     UnaryOperator<String> lookupNameBySourceKey,
                                     StringBuilder colPath) throws IOException {
        if (type instanceof GeoPointType || type instanceof FloatVectorType) {
            return type.implicitCast(parser.list());
        } else {
            ArrayList<Object> values = new ArrayList<>();
            Token token = parser.nextToken();
            // Handles nested arrays
            // ex:
            //   CREATE TABLE test (
            //   "a" array(object as (
            //   "b" array(object as (
            //   "s" string
            //   )))));
            //   SELECT a['b'] from test; -- resolves to array(array(object))
            if (type instanceof ArrayType) {
                type = ((ArrayType<?>) type).innerType();
            }
            for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                values.add(parseValue(parser, type, requiredColumns, droppedColumns, lookupNameBySourceKey, colPath, false));
            }
            return values;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, Object> parseObject(XContentParser parser,
                                                   @Nullable Map<String, Object> requiredColumns,
                                                   Set<String> droppedColumns,
                                                   UnaryOperator<String> lookupNameBySourceKey,
                                                   StringBuilder colPath,
                                                   boolean includeUnknown) throws IOException {
        var parseAllFields = false;
        if (requiredColumns == null || requiredColumns.isEmpty()) {
            parseAllFields = true;
        }
        HashMap<String, Object> values = new HashMap<>();
        XContentParser.Token token = parser.nextToken(); // move past START_OBJECT;
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String fieldName = lookupNameBySourceKey.apply(parser.currentName());
            boolean dropped = false;
            if (droppedColumns.isEmpty() == false) {
                String path = fieldName;
                if (colPath.isEmpty() == false) {
                    path = colPath + "." + fieldName;
                }
                dropped = droppedColumns.contains(path);
            }

            token = parser.nextToken(); // Move to the current field's value
            var required = requiredColumns == null ? null : requiredColumns.get(fieldName);
            if ((parseAllFields == false && required == null && !includeUnknown) || dropped) {
                parser.skipChildren();
            } else if (token == START_ARRAY
                && required instanceof DataType<?>
                && !(required instanceof ArrayType<?>)
                && !(required instanceof GeoPointType)
                && !(required instanceof GeoShapeType)
                && !(required instanceof FloatVectorType)
                && !(required instanceof UndefinedType)) {
                // due to a bug: https://github.com/crate/crate/issues/13990
                parser.skipChildren();
                values.put(fieldName, null);
            } else if (token == VALUE_NULL) {
                // If object value is null, we can short-circuit to NULL and continue further with other fields.
                // We should not call parseObject() as current object's innerTypes can interfere with sibling columns
                // and in case of same names cause parsing errors. See https://github.com/crate/crate/issues/13372
                values.put(fieldName, null);
            } else {
                var prevLength = appendToColPath(colPath, fieldName);

                boolean currentTreeIncludeUnknown = false;
                DataType<?> type = null;
                if (required instanceof DataType<?> dataType) {
                    type = dataType;
                    required = null;
                    if (ArrayType.unnest(dataType) instanceof ObjectType objectType) {
                        // Use inner types to parse the object sub-columns for type aware parsing
                        required = objectType.innerTypes();
                        // When parsing a complete object, we need to parse also possible ignored sub-columns
                        // (We do not know if the object supports ignored sub-columns or not)
                        currentTreeIncludeUnknown = true;
                    }
                }

                Object value = parseValue(
                    parser,
                    type,
                    (Map) required,
                    droppedColumns,
                    lookupNameBySourceKey,
                    colPath,
                    currentTreeIncludeUnknown);
                values.put(fieldName, value);

                colPath.delete(prevLength, colPath.length());
            }
        }
        return values;
    }

    private static int appendToColPath(StringBuilder colPath, String fieldName) {
        var prevLength = colPath.length();
        if (colPath.isEmpty() == false) {
            colPath.append('.');
        }
        colPath.append(fieldName);
        return prevLength;
    }

    /**
     * Parsing should preferable happen based on a non-null given data type even if the value is a string
     * inside the source.
     * Non-string values could be stored as strings inside the _source because we do not sanitize
     * the input on COPY FROM.
     */
    private static Object parseValue(XContentParser parser,
                                     @Nullable DataType<?> type,
                                     @Nullable Map<String, Object> requiredColumns,
                                     Set<String> droppedColumns,
                                     UnaryOperator<String> lookupNameBySourceKey,
                                     StringBuilder colPath,
                                     boolean includeUnknown) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case START_ARRAY -> parseArray(parser, type, requiredColumns, droppedColumns, lookupNameBySourceKey,
                colPath);
            case START_OBJECT -> parseObject(parser, requiredColumns, droppedColumns, lookupNameBySourceKey,
                colPath, includeUnknown);
            case VALUE_STRING -> type == null ? parser.text() : parseByType(parser, type);
            case VALUE_NUMBER -> type == null ? parser.numberValue() : parseByType(parser, type);
            case VALUE_BOOLEAN -> type == null ? parser.booleanValue() : parseByType(parser, type);
            case VALUE_EMBEDDED_OBJECT -> type == null ? parser.binaryValue() : parseByType(parser, type);
            default -> throw new UnsupportedOperationException("Unsupported token encountered, expected a value, got "
                + parser.currentToken());
        };
    }

    private static Object parseByType(XContentParser parser, DataType<?> type) throws IOException {
        assert type != null : "Type must no be null when parsing data type aware";

        // Type could be an array if traversed into an object array → unnest to get the inner type
        var elementType = ArrayType.unnest(type);
        return switch (elementType.id()) {
            case BooleanType.ID -> parser.booleanValue();
            case ByteType.ID -> (byte) parser.intValue();
            case ShortType.ID -> parser.shortValue(true);
            case IntegerType.ID -> parser.intValue();
            case LongType.ID -> parser.longValue();
            case TimestampType.ID_WITH_TZ -> parser.longValue();
            case TimestampType.ID_WITHOUT_TZ -> parser.longValue();
            case FloatType.ID -> parser.floatValue();
            case DoubleType.ID -> parser.doubleValue();
            case BitStringType.ID -> new BitString(
                BitSet.valueOf(parser.binaryValue()),
                ((BitStringType) elementType).length()
            );
            default -> parser.text();
        };
    }
}
