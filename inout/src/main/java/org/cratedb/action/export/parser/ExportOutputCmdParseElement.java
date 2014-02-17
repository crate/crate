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

package org.cratedb.action.export.parser;

import org.cratedb.action.export.ExportContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for token ``output_cmd``. The value of the token might be a String
 * containing the command or an array containing the command and all
 * arguments as seperated parts.
 * <p/>
 * <pre>
 * "output_cmd": "gzip > /tmp/out"
 *
 * or
 *
 * "output_cmd": ["gzip", ">" "/tmp/out"]
 * </pre>
 */
public class ExportOutputCmdParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((ExportContext) context).outputCmd(parser.text());
        } else if (token == XContentParser.Token.START_ARRAY) {
            List<String> cmds = new ArrayList<String>(4);
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                cmds.add(parser.text());
            }
            ((ExportContext) context).outputCmdArray(cmds);
        }
    }
}
