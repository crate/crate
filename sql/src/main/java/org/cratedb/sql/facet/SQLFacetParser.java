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

package org.cratedb.sql.facet;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.parser.SQLXContentSourceContext;
import org.cratedb.action.sql.parser.SQLXContentSourceParser;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.SQLParseException;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;


/**
 *
 */
public class SQLFacetParser extends AbstractComponent implements FacetParser {

    private final TransportUpdateAction updateAction;
    private final SQLParseService parseService;

    @Inject
    public SQLFacetParser(
            Settings settings,
            SQLParseService parseService,
            TransportUpdateAction updateAction) {
        super(settings);
        InternalSQLFacet.registerStreams();
        this.parseService = parseService;
        this.updateAction = updateAction;
    }

    @Override
    public String[] types() {
        return new String[]{
                SQLFacet.TYPE
        };
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor parse(String facetName, XContentParser parser,
            SearchContext searchContext) throws IOException {
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser sqlParser = new SQLXContentSourceParser(context);
        try {
            sqlParser.parse(parser);
        } catch (Exception e) {
            throw new FacetPhaseExecutionException(facetName, "body parse failure", e);
        }

        ParsedStatement stmt;
        try {
            stmt = parseService.parse(context.stmt(), context.args());
        } catch (SQLParseException e) {
            throw new FacetPhaseExecutionException(facetName, "sql parse failure", e);
        }

        return new SQLFacetExecutor(stmt, searchContext, updateAction);
    }
}
