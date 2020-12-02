/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import org.elasticsearch.common.xcontent.XContentType;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import io.crate.types.UndefinedType;

public final class SourceParser {

    private final Map<String, ParsePath> roots = new HashMap<>();
    private int numColumns = 0;
    private Object[] result;

    public SourceParser() {
    }

    static class ParsePath {

        String name;
        DataType<?> type;
        int index;
        List<ParsePath> children;

        public ParsePath(int numColumns, List<String> path, DataType<?> type) {
            assert path.size() > 0 : "Path must have at least 1 element";
            this.name = path.get(0);
            if (path.size() == 1) {
                this.type = ArrayType.unnest(type);
                this.index = numColumns;
            } else {
                this.type = null;
                this.index = -1;
                this.children = new ArrayList<>();
                this.children.add(new ParsePath(numColumns, path.subList(1, path.size()), type));
            }
        }

        public void add(int numColumns, List<String> path, DataType<?> type) {
        }

        @Nullable
        public ParsePath forChild(String fieldName) {
            if (children == null) {
                return null;
            }
            for (int i = 0; i < children.size(); i++) {
                ParsePath child = children.get(i);
                if (child.name.equals(fieldName)) {
                    return child;
                }
            }
            return null;
        }
    }

    public void register(ColumnIdent docColumn, DataType<?> type) {
        assert docColumn.name().equals(DocSysColumns.DOC.name()) && docColumn.path().size() > 0
            : "All columns registered for sourceParser must start with _doc";

        /**
         * We cannot distinguish between `obj_array['x']` and `obj['xs']` without access to the parent type.
         *  - `x` being an integer within an array of objects
         *  - `xs` being an array of integer within an object
         *
         * In both cases, the result should be an array of integers
         *
         * Given that we don't have access to the parent type here, we trust the shape of the JSON.
         * The only thing we control is:
         *
         * - Which paths to follow
         * - The types of the primitive types / leafs
         *
         * Further constraints:
         *
         * - A user selecting a parent object plus one or more child columns explicitly (`SELECT obj, obj['x']`)
         * - Parsing should be cheap (happens per row)
         * - `get` should be cheap (happens per column per row)
         * - `register` can be a bit more expensive (happens per column per query)
         **/
        List<String> path = docColumn.path();
        String start = path.get(0);
        ParsePath parsePath = roots.get(start);
        if (parsePath == null) {
            parsePath = new ParsePath(numColumns, path, type);
            roots.put(start, parsePath);
        } else {
            parsePath.add(numColumns, path, type);
        }
        numColumns++;
    }

    public Object get(ColumnIdent column) {
        assert column.name().equals(DocSysColumns.DOC.name())
            : "All columns retrieved with sourceParser must start with _doc";

        List<String> path = column.path();
        ParsePath parsePath = roots.get(path.get(0));
        if (parsePath == null) {
            return null;
        }
        for (int i = 1; i < path.size(); i++) {
            parsePath = parsePath.forChild(path.get(i));
            if (parsePath == null) {
                return null;
            } else if (i + 1 == path.size()) {
                return result[parsePath.index];
            }
        }
        return parsePath.index >= 0 ? result[parsePath.index] : null;
    }

    public void reset() {
        result = null;
    }

    public boolean parsed() {
        return result != null;
    }

    public void parse(BytesReference bytes) {
        try (InputStream inputStream = XContentHelper.getUncompressedInputStream(bytes)) {
            XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                inputStream
            );
            Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
            }
            result = new Object[numColumns];
            for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if (token == XContentParser.Token.VALUE_NULL) {
                    // result[index] value defaults to null, nothing to do
                } else {
                    ParsePath parsePath = roots.get(fieldName);
                    if (parsePath == null) {
                        parser.skipChildren();
                    } else {
                        traverseParser(parser, parsePath, false);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void traverseParser(XContentParser parser, ParsePath parsePath, boolean parentIsArray) throws IOException {
        if (parsePath.index >= 0) {
            result[parsePath.index] = parseValue(parser, parsePath.type);
            return;
        }

        Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            // Nothing to do here; result[idx] defaults to null
        } else if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
                String fieldName = parser.currentName();
                parser.nextToken();
                ParsePath child = parsePath.forChild(fieldName);
                if (child == null) {
                    parser.skipChildren();
                } else {
                    if (child.index >= 0) {
                        if (parentIsArray) {
                            List<Object> values = (List<Object>) result[child.index];
                            if (values == null) {
                                values = new ArrayList<>();
                                result[child.index] = values;
                            }
                            values.add(parseValue(parser, child.type));
                        } else {
                            result[child.index] = parseValue(parser, child.type);
                        }
                    } else {
                        traverseParser(parser, child, parentIsArray);
                    }
                }
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
            for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                traverseParser(parser, parsePath, true);
            }
        } else if (parsePath.index >= 0) {
            result[parsePath.index] = parseValue(parser, parsePath.type);
        }
    }

    private static Object parseValue(XContentParser parser, DataType<?> type) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case START_ARRAY -> {
                // TODO: This could be an array of geo-points or a geo-point :(
                if (type.equals(DataTypes.GEO_POINT)) {
                    parser.nextToken();
                    final double x = parser.doubleValue();
                    parser.nextToken();
                    final double y = parser.doubleValue();
                    parser.nextToken();
                    yield new PointImpl(x, y, JtsSpatialContext.GEO);
                } else {
                    ArrayList<Object> values = new ArrayList<>();
                    Token token = parser.nextToken();
                    for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                        values.add(parseValue(parser, type));
                    }
                    yield values;
                }
            }
            case START_OBJECT -> {
                // If there are inner types we could still parse it without implicitCast
                yield type.implicitCast(parser.map());
            }
            default -> switch (type.id()) {
                case ByteType.ID -> (byte) parser.shortValue(true);
                case BooleanType.ID -> parser.booleanValue();
                case ShortType.ID -> parser.shortValue(true);
                case IntegerType.ID -> parser.intValue();
                case LongType.ID -> parser.longValue();
                case TimestampType.ID_WITH_TZ -> parser.longValue();
                case TimestampType.ID_WITHOUT_TZ -> parser.longValue();
                case FloatType.ID -> parser.floatValue();
                case DoubleType.ID -> parser.doubleValue();
                case StringType.ID -> parser.text();
                case IpType.ID -> parser.text();
                case GeoPointType.ID -> parser.doubleValue();
                case UndefinedType.ID -> null; // should fallback to generic read?
                default -> {
                    throw new UnsupportedOperationException("Primitive type expected. Cannot parse value for type=" + type);
                }
            };
        };
    }
}
