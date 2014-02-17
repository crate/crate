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

package org.cratedb.action.searchinto.parser;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;

import org.cratedb.action.searchinto.SearchIntoContext;

public abstract class AbstractSearchIntoParser implements ISearchIntoParser {

    /**
     * Main method of this class to parse given payload of _search_into action
     *
     * @param context
     * @param source
     * @throws org.elasticsearch.search.SearchParseException
     *
     */
    public void parseSource(SearchIntoContext context,
            BytesReference source) throws SearchParseException {
        XContentParser parser = null;
        try {
            if (source != null) {
                parser = XContentFactory.xContent(source).createParser(source);
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token
                        .END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        SearchParseElement element = getElementParsers().get(
                                fieldName);
                        if (element == null) {
                            throw new SearchParseException(context,
                                    "No parser for element [" + fieldName +
                                            "]");
                        }
                        element.parse(parser, context);
                    } else if (token == null) {
                        break;
                    }
                }
            }
            validate(context);
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new SearchParseException(context,
                    "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    /**
     * Get the element parser map
     * @return
     */
    protected abstract ImmutableMap<String, SearchParseElement> getElementParsers();

    /**
     * Validate the pay load of the search-into context.
     * @param context
     */
    protected void validate(SearchIntoContext context) {
        if (context.hasFieldNames() && context.fieldNames().contains("_source")) {
            String index = context.mapperService().index().getName();
            for (String type : context.mapperService().types()) {
                if (!context.mapperService().documentMapper(type).sourceMapper().enabled()) {
                    throw new SearchParseException(context,
                            "The _source field of index " + index + " and type " + type + " is not stored.");
                }
            }
        }
    }
}
