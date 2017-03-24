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

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class SQLArgsParseElement implements SQLParseElement {

    @Override
    public void parse(XContentParser parser, SQLXContentSourceContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new SQLParseSourceException("Field [" + parser.currentName() + "] has an invalid value");
        }
        context.args(parseSubArray(parser));
    }

    static Object[] parseSubArray(XContentParser parser)
        throws IOException {
        XContentParser.Token token;
        List<Object> subList = new ArrayList<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token.isValue()) {
                subList.add(parser.objectText());
            } else if (token == XContentParser.Token.START_ARRAY) {
                subList.add(parseSubArray(parser));
            } else if (token == XContentParser.Token.START_OBJECT) {
                subList.add(parser.map());
            } else if (token == XContentParser.Token.VALUE_NULL) {
                subList.add(null);
            } else {
                throw new SQLParseSourceException("Field [" + parser.currentName() + "] has an invalid value");
            }
        }
        return subList.toArray(new Object[subList.size()]);
    }
}
