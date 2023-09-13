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
import java.util.function.Function;
import java.util.stream.Collectors;

import io.crate.metadata.Reference;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;
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

    private final Map<String, Object> requiredColumns = new HashMap<>();
    private final Set<String> droppedColumns;
    private final Function<String, String> lookupNameBySourceKey;
    private boolean registerAll;

    public SourceParser(Set<ColumnIdent> droppedColumns, Function<String, String> lookupNameBySourceKey) {
        // Use a Set of string fqn instead of ColumnIdent to avoid creating ColumnIdent objects to call `contains`
        this.droppedColumns = droppedColumns.stream().map(ColumnIdent::fqn).collect(Collectors.toUnmodifiableSet());
        this.lookupNameBySourceKey = lookupNameBySourceKey;
        this.registerAll = false;
    }

    /**
     * Similar to {@link #register(ColumnIdent, DataType)} but doesn't have _doc semantics
     * so that it's possible to directly register references without creating an intermediate _doc reference.
     * <p>
     * If all columns are required, we need to restrict registration of specific references.
     * It can happen on "SELECT _doc, x..." query.
     * We need to keep requiredColumns clean, so that parsing falls to behavior "return source as map".
     */
    public void register(Reference reference) {
        if (!registerAll) {
            List<String> path = reference.column().path();
            if (path.isEmpty()) {
                requiredColumns.put(reference.column().name(), reference.valueType());
            } else if (path.size() == 1) {
                HashMap<String, Object> children = new HashMap<>();
                children.put(path.get(0), reference.valueType());
                requiredColumns.put(reference.column().name(), children);
            } else {
                registerPath(reference.column().path(), reference.valueType());
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void register(ColumnIdent docColumn, DataType<?> type) {
        assert docColumn.name().equals(DocSysColumns.DOC.name()) && docColumn.path().size() > 0
            : "All columns registered for sourceParser must start with _doc";

        List<String> path = docColumn.path();
        if (path.size() == 1) {
            requiredColumns.put(docColumn.path().get(0), type);
        } else {
            registerPath(path, type);
        }
    }

    private void registerPath(List<String> path, DataType<?> type) {
        assert path.size() >= 2 : "Path size must be at least 2 to ensure that leaf type is wrapped into a map";
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

    /**
     * _doc needs registering of ALL columns.
     * By default, if no column is registered, SourceParser falls back to such behavior.
     * In case of mixed select (both _doc and some regular column),
     * parser would see only specific column and select _doc will be broken.
     * We need to ensure that _doc selection makes all columns required.
     */
    public void registerAll() {
        registerAll = true;
        requiredColumns.clear();
    }

    public Map<String, Object> parse(BytesReference bytes, boolean includeUnknownCols) {
        try (InputStream inputStream = XContentHelper.getUncompressedInputStream(bytes)) {
            XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                inputStream
            );
            Token token = parser.currentToken();
            if (token == null) {
                parser.nextToken();
            }
            return parseObject(
                parser,
                null,
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
                                     Function<String, String> lookupNameBySourceKey,
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
            while (type instanceof ArrayType) {
                type = ((ArrayType<?>) type).innerType();
            }
            for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                values.add(parseValue(parser, type, requiredColumns, droppedColumns, lookupNameBySourceKey, colPath));
            }
            return values;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, Object> parseObject(XContentParser parser,
                                                   @Nullable DataType<?> type,
                                                   @Nullable Map<String, Object> requiredColumns,
                                                   Set<String> droppedColumns,
                                                   Function<String, String> lookupNameBySourceKey,
                                                   StringBuilder colPath,
                                                   boolean includeUnknown) throws IOException {
        if (requiredColumns == null || requiredColumns.isEmpty()) {
            return type == null ? parser.map() : (Map) type.implicitCast(parser.map());
        } else {
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
                var required = requiredColumns.get(fieldName);
                if ((required == null && !includeUnknown) || dropped) {
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
                } else if (required instanceof ObjectType objectType) {
                    var prevLength = appendToColPath(colPath, fieldName);
                    values.put(fieldName, parseObject(
                        parser,
                        objectType,
                        (Map) objectType.innerTypes(),
                        droppedColumns,
                        lookupNameBySourceKey,
                        colPath,
                        true)
                    );
                    colPath.delete(prevLength, colPath.length());
                } else if (required instanceof DataType<?> dataType) {
                    if (dataType instanceof ArrayType<?> arrayType && arrayType.innerType().id() == ObjectType.ID) {
                        var prevLength = appendToColPath(colPath, fieldName);
                        values.put(fieldName, parseValue(parser, arrayType.innerType(),
                            (Map) ((ObjectType) arrayType.innerType()).innerTypes(), droppedColumns,
                            lookupNameBySourceKey, colPath)
                        );
                        colPath.delete(prevLength, colPath.length());
                    } else {
                        values.put(fieldName, parseValue(parser, dataType, null, droppedColumns,
                            lookupNameBySourceKey, colPath)
                        );
                    }
                } else {
                    values.put(fieldName, parseValue(parser, null, (Map) required, droppedColumns,
                        lookupNameBySourceKey, colPath));
                }
            }
            return values;
        }
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
                                     Function<String, String> lookupNameBySourceKey,
                                     StringBuilder colPath) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case START_ARRAY -> parseArray(parser, type, requiredColumns, droppedColumns, lookupNameBySourceKey,
                colPath);
            case START_OBJECT -> parseObject(parser, type, requiredColumns, droppedColumns, lookupNameBySourceKey,
                colPath, false);
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
