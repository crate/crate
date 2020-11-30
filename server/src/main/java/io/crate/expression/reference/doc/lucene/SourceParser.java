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
import java.util.Map;

import javax.annotation.Nullable;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.StringType;

public final class SourceParser {

    private final Map<ColumnIdent, ParseInfo> parseInfoByColumn = new HashMap<>();
    private Object[] result;

    public SourceParser() {
    }

    static class ParseInfo {

        final DataType<?> type;
        final int index;

        public ParseInfo(DataType<?> type, int index) {
            this.type = type;
            this.index = index;
        }
    }

    Object parseValue(XContentParser parser, ParseInfo parseInfo, ColumnIdent column) throws IOException {
        return switch (parseInfo.type.id()) {
            case ArrayType.ID -> parseArray(parser, parseInfo, column);
            case ObjectType.ID -> parseObject(parser, parseInfo, column);
            case ShortType.ID -> parser.shortValue(true);
            case IntegerType.ID -> parser.intValue();
            case LongType.ID -> parser.longValue();
            case FloatType.ID -> parser.floatValue();
            case DoubleType.ID -> parser.doubleValue();
            case StringType.ID -> parser.text();
            default -> {
                throw new UnsupportedOperationException("parseValue not implemented for " + parseInfo.type);
            }
        };
    }

    Object parseObject(XContentParser parser, ParseInfo parseInfo, ColumnIdent column) throws IOException {
        Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        assert parseInfo.type instanceof ObjectType
            : "type passed to parseObject must be an ObjectType. Got=" + parseInfo.type;
        ObjectType objectType = (ObjectType) parseInfo.type;
        HashMap<String, Object> result = new HashMap<>();
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String fieldName = parser.currentName();
            parser.nextToken();

            var childColumn = column.append(fieldName);
            if (token == XContentParser.Token.VALUE_NULL) {
                result.put(fieldName, null);
            } else {
                var childType = objectType.innerType(fieldName);
                // Object value = parseValue(typesByColumn, parser, childType, childColumn);
                // result.put(fieldName, value);
            }
        }
        return result;
    }

    Object parseArray(XContentParser parser, ParseInfo parseInfo, ColumnIdent column) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        } else {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "Failed to parse list:  expecting "
                + XContentParser.Token.START_ARRAY + " but got " + token);
        }
        assert parseInfo.type instanceof ArrayType
            : "type passed to parseArray must be an ArrayType. Got=" + parseInfo.type;
        DataType<?> innerType = ((ArrayType<?>) parseInfo.type).innerType();
        ArrayList<Object> list = new ArrayList<>();
        for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
            if (token == XContentParser.Token.VALUE_NULL) {
                list.add(null);
            } else {
                //list.add(parseValue(parser, innerType, column));
            }
        }
        return list;
    }

    public void register(Reference ref) {
        ColumnIdent docColumn = ref.column();
        assert docColumn.name().equals(DocSysColumns.DOC.name()) : "All columns registered for sourceParser must start with _doc";
        ColumnIdent column = docColumn.shiftRight();

        if (column.isTopLevel()) {
            ParseInfo parseInfo = parseInfoByColumn.get(column);
            if (parseInfo != null) {
                return;
            }
            parseInfo = new ParseInfo(ref.valueType(), parseInfoByColumn.size());
            parseInfoByColumn.put(column, parseInfo);
        } else {
        }
    }

    public Object get(ColumnIdent column) {
        ParseInfo parseInfo = parseInfoByColumn.get(column.shiftRight());
        return parseInfo == null
            ? null
            : result[parseInfo.index];
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
            result = new Object[parseInfoByColumn.size()];
            for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
                String fieldName = parser.currentName();
                ColumnIdent column = new ColumnIdent(fieldName);
                parser.nextToken();
                if (token == XContentParser.Token.VALUE_NULL) {
                    // result[index] value defaults to null, nothing to do
                } else {
                    ParseInfo parseInfo = parseInfoByColumn.get(column);
                    if (parseInfo == null) {
                        parser.skipChildren();
                        continue;
                    }
                    Object value = parseValue(parser, parseInfo, column);
                    if (parseInfo.index >= 0) {
                        result[parseInfo.index] = value;
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
