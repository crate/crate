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

package io.crate.rest.action;

import io.crate.action.sql.SQLRequestBuilder;
import io.crate.action.sql.SQLResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

public class RestSQLAction extends BaseRestHandler {

    @Inject
    public RestSQLAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.POST, "/_sql", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {

        final SQLRequestBuilder requestBuilder = new SQLRequestBuilder(client);
        try {
            if (request.hasContent()) {
                requestBuilder.source(request.content());
            } else {
                throw new ElasticsearchException("missing request body");
            }
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("failed to parse sql request", e);
            }
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error",
                        e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        requestBuilder.execute(new ActionListener<SQLResponse>() {
            @Override
            public void onResponse(SQLResponse response) {

                try {
                    XContentBuilder builder = restContentBuilder(request);
                    response.toXContent(builder, request);
                    // TODO: implement shard counts on response to set status properly
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to execute search (building response)", e);
                    }
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new CrateThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}