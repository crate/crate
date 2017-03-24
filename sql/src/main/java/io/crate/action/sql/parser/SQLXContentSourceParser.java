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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nonnull;

/**
 * Parser for SQL statements in JSON and other XContent formats
 * <p>
 * <pre>
 * {
 *  "stmt": "select * from...."
 * }
 *     </pre>
 */
public final class SQLXContentSourceParser {

    static final class Fields {
        static final String STMT = "stmt";
        static final String ARGS = "args";
        static final String BULK_ARGS = "bulk_args";
    }

    private static final ImmutableMap<String, SQLParseElement> ELEMENT_PARSERS = ImmutableMap.of(
        Fields.STMT, (SQLParseElement) new SQLStmtParseElement(),
        Fields.ARGS, (SQLParseElement) new SQLArgsParseElement(),
        Fields.BULK_ARGS, (SQLParseElement) new SQLBulkArgsParseElement()
    );

    private SQLXContentSourceParser() {
    }

    private static SQLXContentSourceContext ensureHasStmt(SQLXContentSourceContext context) throws SQLParseSourceException {
        if (context.stmt() == null) {
            throw new SQLParseSourceException("Field [stmt] was not defined");
        }
        return context;
    }

    public static SQLXContentSourceContext parseSource(@Nonnull BytesReference source) throws SQLParseException {
        XContentParser parser = null;
        try {
            parser = JsonXContent.jsonXContent.createParser(source);
            return ensureHasStmt(parse(parser));
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
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

    public static SQLXContentSourceContext parse(XContentParser parser) throws Exception {
        XContentParser.Token token;
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken();
                SQLParseElement element = ELEMENT_PARSERS.get(fieldName);
                if (element == null) {
                    throw new SQLParseException("No parser for element [" + fieldName + "]");
                }
                element.parse(parser, context);
            } else if (token == null) {
                break;
            }
        }
        return context;
    }
}
