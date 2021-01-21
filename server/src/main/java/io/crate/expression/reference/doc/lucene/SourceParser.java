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
import org.elasticsearch.common.xcontent.XContentType;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;

public final class SourceParser {

    private static final Object FULL_OBJECT = new Object();
    private final Map<String, Object> requiredColumns = new HashMap<>();

    public SourceParser() {
    }

    public void register(ColumnIdent docColumn) {
        assert docColumn.name().equals(DocSysColumns.DOC.name()) && docColumn.path().size() > 0
            : "All columns registered for sourceParser must start with _doc";

        List<String> path = docColumn.path();
        if (path.size() == 1) {
            requiredColumns.put(docColumn.path().get(0), FULL_OBJECT);
        } else {
            Map<String, Object> columns = requiredColumns;
            for (int i = 0; i < path.size(); i++) {
                String part = path.get(i);
                if (i + 1 == path.size()) {
                    columns.put(part, FULL_OBJECT);
                } else {
                    Object object = columns.get(part);
                    if (object instanceof Map) {
                        columns = (Map) object;
                    } else if (object == FULL_OBJECT) {
                        break;
                    } else {
                        HashMap<String, Object> children = new HashMap<String, Object>();
                        columns.put(part, children);
                        columns = children;
                    }
                }
            }
        }
    }

    public Map<String, Object> parse(BytesReference bytes) {
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
            Map<String, Object> result = new HashMap<>();
            for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
                String fieldName = parser.currentName();
                parser.nextToken();
                // empty means the full _doc is required
                if (requiredColumns.isEmpty()) {
                    result.put(fieldName, parseValue(parser, null));
                } else {
                    Object required = requiredColumns.get(fieldName);
                    if (required == null) {
                        parser.skipChildren();
                    } else if (required == FULL_OBJECT) {
                        result.put(fieldName, parseValue(parser, null));
                    } else {
                        assert required instanceof Map
                            : "requiredColumns must either contain the FULL_OBJECT marker or a Map with child columns to load";

                        result.put(fieldName, parseValue(parser, (Map) required));
                    }
                }
            }
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Object parseValue(XContentParser parser, @Nullable Map<String, Object> requiredColumns) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case START_ARRAY -> {
                ArrayList<Object> values = new ArrayList<>();
                Token token = parser.nextToken();
                for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                    values.add(parseValue(parser, requiredColumns));
                }
                yield values;
            }
            case START_OBJECT -> {
                if (requiredColumns == null) {
                    yield parser.map();
                } else {
                    HashMap<String, Object> values = new HashMap<>();
                    XContentParser.Token token = parser.nextToken(); // move past START_OBJECT;
                    for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        var required = requiredColumns.get(fieldName);
                        if (required == null) {
                            parser.skipChildren();
                        } else if (required == FULL_OBJECT) {
                            values.put(fieldName, parseValue(parser, null));
                        } else {
                            values.put(fieldName, parseValue(parser, (Map) required));
                        }
                    }
                    yield values;
                }
            }
            case VALUE_STRING -> parser.text();
            case VALUE_NUMBER -> parser.numberValue();
            case VALUE_BOOLEAN -> parser.booleanValue();
            case VALUE_EMBEDDED_OBJECT -> parser.binaryValue();
            default -> {
                throw new UnsupportedOperationException("Unsupported token encountered, expected a value, got " + parser.currentToken());
            }
        };
    }
}
