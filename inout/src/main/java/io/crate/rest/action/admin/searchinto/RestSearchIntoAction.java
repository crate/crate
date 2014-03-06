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

package io.crate.rest.action.admin.searchinto;

import io.crate.action.searchinto.SearchIntoAction;
import io.crate.action.searchinto.SearchIntoRequest;
import io.crate.action.searchinto.SearchIntoResponse;
import io.crate.client.action.searchinto.SearchIntoRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestSearchIntoAction extends BaseRestHandler {

    @Inject
    public RestSearchIntoAction(Settings settings, Client client,
            RestController controller) {
        super(settings, client);
        registerHandlers(controller);
    }

    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_search_into", this);
        controller.registerHandler(POST, "/{index}/_search_into", this);
        controller.registerHandler(POST, "/{index}/{type}/_search_into", this);
    }

    protected Action<SearchIntoRequest, SearchIntoResponse, SearchIntoRequestBuilder> action() {
        return SearchIntoAction.INSTANCE;
    }

    public void handleRequest(final RestRequest request,
            final RestChannel channel) {
        SearchIntoRequest searchIntoRequest = new SearchIntoRequest(
            Strings.splitStringByCommaToArray(request.param("index")));

        if (request.hasParam("ignore_unavailable") ||
                request.hasParam("allow_no_indices") ||
                request.hasParam("expand_wildcards")) {
            IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.lenient());
            searchIntoRequest.indicesOptions(indicesOptions);
        }
        else if (request.hasParam("ignore_indices")) {
            if (request.param("ignore_indices").equalsIgnoreCase("missing")) {
                searchIntoRequest.indicesOptions(IndicesOptions.lenient());
            }
            else {
                searchIntoRequest.indicesOptions(IndicesOptions.strict());
            }
        }
        searchIntoRequest.listenerThreaded(false);
        try {
            BroadcastOperationThreading operationThreading =
                    BroadcastOperationThreading.fromString(
                            request.param("operation_threading"),
                            BroadcastOperationThreading.SINGLE_THREAD);
            if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
                // since we don't spawn, don't allow no_threads,
                // but change it to a single thread
                operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
            }
            searchIntoRequest.operationThreading(operationThreading);
            if (request.hasContent()) {
                searchIntoRequest.source(request.content(),
                        request.contentUnsafe());
            } else {
                String source = request.param("source");
                if (source != null) {
                    searchIntoRequest.source(source);
                } else {
                    BytesReference querySource = RestActions.parseQuerySource(
                            request).buildAsBytes(XContentType.JSON);
                    if (querySource != null) {
                        searchIntoRequest.source(querySource, false);
                    }
                }
            }
            searchIntoRequest.routing(request.param("routing"));
            searchIntoRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
            searchIntoRequest.preference(request.param("preference",
                    "_primary"));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder
                        .restContentBuilder(
                                request);
                channel.sendResponse(new XContentRestResponse(request,
                        BAD_REQUEST, builder.startObject().field("error",
                        e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.execute(action(), searchIntoRequest,
                new ActionListener<SearchIntoResponse>() {

                    public void onResponse(SearchIntoResponse response) {
                        try {
                            XContentBuilder builder = RestXContentBuilder
                                    .restContentBuilder(
                                            request);
                            response.toXContent(builder, request);
                            channel.sendResponse(new XContentRestResponse(
                                    request, OK, builder));
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(
                                    new XContentThrowableRestResponse(request,
                                            e));
                        } catch (IOException e1) {
                            logger.error("Failed to send failure response",
                                    e1);
                        }
                    }
                });

    }
}
