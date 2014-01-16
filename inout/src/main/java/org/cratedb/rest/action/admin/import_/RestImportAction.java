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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.rest.action.admin.import_;

import org.cratedb.action.import_.ImportAction;
import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.import_.ImportResponse;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestImportAction extends BaseRestHandler {

    @Inject
    public RestImportAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        registerHandlers(controller);
    }

    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_import", this);
        controller.registerHandler(POST, "/{index}/_import", this);
        controller.registerHandler(POST, "/{index}/{type}/_import", this);
    }


    protected Action action() {
        return ImportAction.INSTANCE;
    }

    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ImportRequest importRequest = new ImportRequest();
        importRequest.listenerThreaded(false);
        try {
            if (request.hasContent()) {
                importRequest.source(request.content(), request.contentUnsafe());
            } else {
                String source = request.param("source");
                if (source != null) {
                    importRequest.source(source);
                } else {
                    BytesReference querySource = RestActions.parseQuerySource(request);
                    if (querySource != null) {
                        importRequest.source(querySource, false);
                    }
                }
            }
            importRequest.index(request.param("index"));
            importRequest.type(request.param("type"));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }


        client.execute(action(), importRequest, new ActionListener<ImportResponse>() {

            public void onResponse(ImportResponse response) {
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
