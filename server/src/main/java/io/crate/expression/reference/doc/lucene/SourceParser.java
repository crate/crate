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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StringType;

public final class SourceParser {

    private final Map<String, ParsePath> roots = new HashMap<>();
    private int numColumns = 0;
    private Object[] result;

    public SourceParser() {
    }

    static class ParsePath {

        DataType<?> type;
        int index;

		public ParsePath forChild(String fieldName) {
			return null;
		}

		public void add(int numColumns, List<String> list, DataType<?> type) {
		}

		public void add(int numColumns, DataType<?> type) {
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
            parsePath = new ParsePath();
            roots.put(start, parsePath);
        }
        if (path.size() == 1) {
            parsePath.add(numColumns, type);
        } else {
            parsePath.add(numColumns, path.subList(1, path.size()), type);
        }
        numColumns++;
    }

    public Object get(ColumnIdent column) {
        assert column.name().equals(DocSysColumns.DOC.name())
            : "All columns retrieved with sourceParser must start with _doc";

        return null;
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
                        parseValue(parser, parsePath, 0);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void parseValue(XContentParser parser, ParsePath parsePath, int level) throws IOException {
        Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
        } else if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
                String fieldName = parser.currentName();
                parser.nextToken();
                ParsePath child = parsePath.forChild(fieldName);
                if (child == null) {
                    parser.skipChildren();
                } else {
                    parseValue(parser, child, level++);
                }
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
            // create list here and work with consumer to add value to result?
            for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                parseValue(parser, parsePath, level); // level++ ?
            }
        } else {
            result[parsePath.index] = switch (parsePath.type.id()) {
                case ShortType.ID -> parser.shortValue(true);
                case IntegerType.ID -> parser.intValue();
                case LongType.ID -> parser.longValue();
                case FloatType.ID -> parser.floatValue();
                case DoubleType.ID -> parser.doubleValue();
                case StringType.ID -> parser.text();
                default -> {
                    throw new UnsupportedOperationException("PrimitiveTypeParser cannot parse value for type " + parsePath.type);
                }
            };
        }
    }
}
