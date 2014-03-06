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

package io.crate.blob.rest;

import io.crate.blob.stats.BlobStatsAction;
import io.crate.blob.stats.BlobStatsRequest;
import io.crate.blob.stats.BlobStatsResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestBlobIndicesStatsAction extends BaseRestHandler {

    @Inject
    public RestBlobIndicesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_blobs/_status", this);
        controller.registerHandler(GET, "/{index}/_blobs/_status", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        BlobStatsRequest blobStatsRequest = new BlobStatsRequest();
        blobStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));

        client.execute(BlobStatsAction.INSTANCE, blobStatsRequest,
            new ActionListener<BlobStatsResponse>() {

            @Override
            public void onResponse(BlobStatsResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    buildBroadcastShardsHeader(builder, response);
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
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
