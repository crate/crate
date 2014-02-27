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

package org.cratedb.rest.action.admin.export;

import org.cratedb.action.export.ExportAction;
import org.cratedb.action.export.ExportRequest;
import org.cratedb.action.export.ExportResponse;
import org.cratedb.client.action.export.ExportRequestBuilder;
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
public class RestExportAction extends BaseRestHandler {

    @Inject
    public RestExportAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        registerHandlers(controller);
    }

    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_export", this);
        controller.registerHandler(POST, "/{index}/_export", this);
        controller.registerHandler(POST, "/{index}/{type}/_export", this);
    }

    protected Action<ExportRequest, ExportResponse, ExportRequestBuilder> action() {
        return ExportAction.INSTANCE;
    }

    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ExportRequest exportRequest = new ExportRequest(Strings.splitStringByCommaToArray(request.param("index")));

        if (request.hasParam("ignore_unavailable") ||
                request.hasParam("allow_no_indices") ||
                request.hasParam("expand_wildcards")) {
            IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.lenient());
            exportRequest.indicesOptions(indicesOptions);
        }
        else if (request.hasParam("ignore_indices")) {
            if (request.param("ignore_indices").equalsIgnoreCase("missing")) {
                exportRequest.indicesOptions(IndicesOptions.lenient());
            }
            else {
                exportRequest.indicesOptions(IndicesOptions.strict());
            }
        }
        exportRequest.listenerThreaded(false);
        try {
            BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operation_threading"), BroadcastOperationThreading.SINGLE_THREAD);
            if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
                // since we don't spawn, don't allow no_threads, but change it to a single thread
                operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
            }
            exportRequest.operationThreading(operationThreading);
            if (request.hasContent()) {
                exportRequest.source(request.content(), request.contentUnsafe());
            } else {
                String source = request.param("source");
                if (source != null) {
                    exportRequest.source(source);
                } else {
                    BytesReference querySource = RestActions.parseQuerySource(request).buildAsBytes(XContentType.JSON);
                    if (querySource != null) {
                        exportRequest.source(querySource, false);
                    }
                }
            }
            exportRequest.routing(request.param("routing"));
            exportRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
            exportRequest.preference(request.param("preference", "_primary"));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.execute(action(), exportRequest, new ActionListener<ExportResponse>() {

            public void onResponse(ExportResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    response.toXContent(builder, request);
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });

    }
}
