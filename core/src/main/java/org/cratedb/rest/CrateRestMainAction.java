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

package org.cratedb.rest;

import org.apache.lucene.util.Constants;
import org.cratedb.Build;
import org.cratedb.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.main.RestMainAction;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class CrateRestMainAction extends BaseRestHandler {

    private final Version version;
    private final RestController controller;

    @Inject
    public CrateRestMainAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        this.version = Version.CURRENT;
        this.controller = controller;
        registerHandler();
    }

    public void registerHandler() {
        controller.registerHandler(GET, "/", this);
        controller.registerHandler(HEAD, "/", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.listenerThreaded(false);
        clusterStateRequest.masterNodeTimeout(TimeValue.timeValueMillis(0));
        clusterStateRequest.local(true);
        clusterStateRequest.filterAll().filterBlocks(false);
        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(ClusterStateResponse response) {
                RestStatus status = RestStatus.OK;
                if (response.getState().blocks().hasGlobalBlock(RestStatus.SERVICE_UNAVAILABLE)) {
                    status = RestStatus.SERVICE_UNAVAILABLE;
                }
                if (request.method() == RestRequest.Method.HEAD) {
                    channel.sendResponse(new StringRestResponse(status));
                    return;
                }

                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request).prettyPrint();
                    builder.startObject();
                    builder.field("ok", true);
                    builder.field("status", status.getStatus());
                    if (settings.get("name") != null) {
                        builder.field("name", settings.get("name"));
                    }
                    builder.startObject("version")
                            .field("number", version.number())
                            .field("build_hash", Build.CURRENT.hash())
                            .field("build_timestamp", Build.CURRENT.timestamp())
                            .field("build_snapshot", version.snapshot)
                            .field("es_version", version.esVersion)
                                    // We use the lucene version from lucene constants since
                                    // this includes bugfix release version as well and is already in
                                    // the right format. We can also be sure that the format is maitained
                                    // since this is also recorded in lucene segments and has BW compat
                            .field("lucene_version", Constants.LUCENE_MAIN_VERSION)
                            .endObject();
                    //builder.field("tagline", "You Know, for Search");
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, status, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    if (request.method() == HEAD) {
                        channel.sendResponse(new StringRestResponse(ExceptionsHelper.status(e)));
                    } else {
                        channel.sendResponse(new XContentThrowableRestResponse(request, e));
                    }
                } catch (Exception e1) {
                    logger.warn("Failed to send response", e);
                }
            }
        });
    }
}
