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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.explain.ExplainParseElement;
import org.elasticsearch.search.query.QueryPhase;

import io.crate.action.searchinto.SearchIntoContext;

/**
 * Parser for payload given to _search_into action.
 */
public class SearchIntoParser extends AbstractSearchIntoParser implements ISearchIntoParser {

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject
    public SearchIntoParser(QueryPhase queryPhase, FetchPhase fetchPhase) {
        Map<String, SearchParseElement> elementParsers = new HashMap<String,
                SearchParseElement>();
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.put("fields", new FieldsParseElement());
        elementParsers.put("targetNodes", new TargetNodesParseElement());
        elementParsers.put("explain", new ExplainParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    @Override
    protected void validate(SearchIntoContext context) {
        if (!context.hasFieldNames()) {
            throw new SearchParseException(context, "No fields defined");
        }

        for (String field : context.fieldNames()) {
            FieldMapper<?> mapper = context.mapperService().smartNameFieldMapper(
                    field);
            if (mapper == null && !field.equals(
                    "_version") && !field.startsWith(
                    FieldsParseElement.SCRIPT_FIELD_PREFIX)) {
                throw new SearchParseException(context,
                        "SearchInto field [" + field + "] does not exist in " +
                                "the mapping");
            }
        }
        super.validate(context);
    }

    @Override
    protected ImmutableMap<String, SearchParseElement> getElementParsers() {
        return elementParsers;
    }


}