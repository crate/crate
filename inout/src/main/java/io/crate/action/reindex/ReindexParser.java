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

package io.crate.action.reindex;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.explain.ExplainParseElement;
import org.elasticsearch.search.query.QueryPhase;

import io.crate.action.searchinto.SearchIntoContext;
import io.crate.action.searchinto.parser.AbstractSearchIntoParser;
import io.crate.action.searchinto.parser.ISearchIntoParser;

/**
 * Parser for pay load given to _reindex action.
 */
public class ReindexParser extends AbstractSearchIntoParser implements ISearchIntoParser {

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject
    public ReindexParser(QueryPhase queryPhase, FetchPhase fetchPhase) {
        Map<String, SearchParseElement> elementParsers = new HashMap<String,
                SearchParseElement>();
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.put("explain", new ExplainParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    @Override
    protected ImmutableMap<String, SearchParseElement> getElementParsers() {
        return elementParsers;
    }

    @Override
    public void parseSource(SearchIntoContext context, BytesReference source)
            throws SearchParseException {
        context.fieldNames().add("_id");
        context.fieldNames().add("_source");
        context.outputNames().put("_id", "_id");
        context.outputNames().put("_source", "_source");
        super.parseSource(context, source);
    }
}
