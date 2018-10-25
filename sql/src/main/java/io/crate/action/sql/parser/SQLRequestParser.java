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

package io.crate.action.sql.parser;

import com.google.common.collect.ImmutableMap;
import io.crate.exceptions.SQLParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * Parser for SQL statements in JSON and other XContent formats
 * <p>
 * <pre>
 * {
 *  "stmt": "select * from...."
 * }
 *     </pre>
 */
public final class SQLRequestParser {

    static final class Fields {
        static final String STMT = "stmt";
        static final String ARGS = "args";
        static final String BULK_ARGS = "bulk_args";
    }

    private static final ImmutableMap<String, SQLParseElement> elementParsers = ImmutableMap.of(
        Fields.STMT, (SQLParseElement) new SQLStmtParseElement(),
        Fields.ARGS, (SQLParseElement) new SQLArgsParseElement(),
        Fields.BULK_ARGS, (SQLParseElement) new SQLBulkArgsParseElement()
    );

    private SQLRequestParser() {
    }

    private static void validate(SQLRequestParseContext parseContext) throws SQLParseSourceException {
        if (parseContext.stmt() == null) {
            throw new SQLParseSourceException("Field [stmt] was not defined");
        }
    }

    public static SQLRequestParseContext parseSource(BytesReference source) throws IOException {
        if (source.length() == 0) {
            throw new SQLParseException("Missing request body");
        }
        XContentParser parser = null;
        try {
            SQLRequestParseContext parseContext = new SQLRequestParseContext();
            parser = XContentFactory.xContent(XContentType.JSON).createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, BytesReference.toBytes(source));
            parse(parseContext, parser);
            validate(parseContext);
            return parseContext;
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false, XContentType.JSON);
            } catch (Throwable e1) {
                // ignore
            }
            throw new SQLParseException("Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public static void parse(SQLRequestParseContext parseContext, XContentParser parser) throws Exception {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken();
                SQLParseElement element = elementParsers.get(fieldName);
                if (element == null) {
                    throw new SQLParseException("No parser for element [" + fieldName + "]");
                }
                element.parse(parser, parseContext);
            } else if (token == null) {
                break;
            }
        }
    }
}
