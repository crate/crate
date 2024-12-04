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

package io.crate.session.parser;

import org.elasticsearch.common.xcontent.XContentParser;

/**
 * used to parse the "stmt" element that is expected to be in requests
 * parsed by the io.crate.action.sql.parser.SQLRequestParser
 * <p>
 * Fills the stmt in the io.crate.action.sql.parser.SQLRequestParseContext.
 * </p>
 */
class SQLStmtParseElement implements SQLParseElement {

    @Override
    public void parse(XContentParser parser, SQLRequestParseContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();

        if (!token.isValue()) {
            throw new SQLParseSourceException("Field [" + parser.currentName() + "] has an invalid value");
        }
        String stmt = parser.text();
        if (stmt == null || stmt.length() == 0) {
            throw new SQLParseSourceException("Field [" + parser.currentName() + "] has no value");
        }
        context.stmt(parser.text());
    }
}
