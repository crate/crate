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

package io.crate.action.searchinto.parser;

import io.crate.action.searchinto.SearchIntoContext;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * parses the targetNode field which looks like
 * <p/>
 * "targetNode": ["host:4300", "host:4301"]
 *  <p/>
 * or
 *  <p/>
 * "targetNode": "host:4300"
 *
 */
public class TargetNodesParseElement implements SearchParseElement {

    private Pattern PATTERN = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            boolean added = false;
            SearchIntoContext ctx = (SearchIntoContext)context;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                addAddress(ctx, parser.text());
            }
            if (!added) {
                ctx.emptyTargetNodes();
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            addAddress((SearchIntoContext)context, parser.text());
        }
    }

    private void addAddress(SearchIntoContext context, String nodeAddress) {
        Matcher m = PATTERN.matcher(nodeAddress);
        if (m.matches()) {
            String host = m.group(1);
            int port = Integer.parseInt(m.group(2));
            InetSocketTransportAddress isa = new InetSocketTransportAddress(host, port);
            context.targetNodes().add(isa);
        } else {
            throw new InvalidNodeAddressException(context, nodeAddress);
        }
    }
}
