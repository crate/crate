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
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

public class SQLFacetExecutor extends FacetExecutor {

    private final UpdateCollector collector;

    public SQLFacetExecutor(
            ParsedStatement stmt,
            SearchContext searchContext,
            TransportUpdateAction updateAction) {
        this.collector = new UpdateCollector(
                stmt.updateDoc(),
                updateAction,
                searchContext,
                stmt.versionFilter);
    }

    /**
     * Executed once per shard after all records are processed
     */
    @Override
    public InternalFacet buildFacet(String facetName) {
        InternalSQLFacet facet = new InternalSQLFacet(facetName);
        facet.rowCount(collector.rowCount());
        return facet;
    }

    @Override
    public Collector collector() {
        return collector;
    }


}
