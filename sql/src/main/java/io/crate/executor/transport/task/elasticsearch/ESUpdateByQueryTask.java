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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.AsyncChainedTask;
import io.crate.executor.transport.task.elasticsearch.facet.InternalUpdateFacet;
import io.crate.executor.transport.task.elasticsearch.facet.UpdateFacet;
import io.crate.planner.node.dml.ESUpdateNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;

import java.io.IOException;
import java.util.UUID;

public class ESUpdateByQueryTask extends AsyncChainedTask {

    static class UpdateByQueryResponseListener implements ActionListener<SearchResponse> {

        private final SettableFuture<TaskResult> future;

        UpdateByQueryResponseListener(SettableFuture<TaskResult> future) {
            this.future = future;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            InternalUpdateFacet facet = searchResponse.getFacets().facet(InternalUpdateFacet.class, UpdateFacet.TYPE);
            facet.reduce();
            future.set(new RowCountResult(facet.rowCount()));
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }

    private final TransportSearchAction transport;
    private final ActionListener<SearchResponse> listener;
    private final SearchRequest request;
    private final ESQueryBuilder queryBuilder;

    public ESUpdateByQueryTask(UUID jobId, TransportSearchAction transport, ESUpdateNode node) {
        super(jobId);
        this.transport = transport;
        this.queryBuilder = new ESQueryBuilder();

        this.request = buildRequest(node);
        this.listener = new UpdateByQueryResponseListener(result);
    }

    private SearchRequest buildRequest(ESUpdateNode node) {

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(node.indices());
        searchRequest.types(Constants.DEFAULT_MAPPING_TYPE);
        searchRequest.preference("_primary");

        searchRequest.routing(node.whereClause().clusteredBy().orNull());
        try {
            searchRequest.source(queryBuilder.convert(node), false);
        } catch (IOException e) {
            result.setException(e);
        }

        return searchRequest;
    }

    @Override
    public void start() {
        transport.execute(this.request, this.listener);
    }
}
