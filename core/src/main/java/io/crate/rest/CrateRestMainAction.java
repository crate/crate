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

package io.crate.rest;

import io.crate.Build;
import io.crate.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class CrateRestMainAction extends BaseRestHandler {

    public static final String PATH = "/";

    private final Version version;
    private final RestController controller;
    private final ClusterName clusterName;
    private final ClusterService clusterService;

    @Inject
    public CrateRestMainAction(Settings settings,
                               RestController controller,
                               ClusterService clusterService) {
        super(settings);
        this.version = Version.CURRENT;
        this.controller = controller;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
    }

    public void registerHandler() {
        controller.registerHandler(GET, PATH, this);
        controller.registerHandler(HEAD, PATH, this);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws
        IOException {
        final RestStatus status;
        if (clusterService.state().blocks().hasGlobalBlock(RestStatus.SERVICE_UNAVAILABLE)) {
            status = RestStatus.SERVICE_UNAVAILABLE;
        } else {
            status = RestStatus.OK;
        }

        if (request.method() == RestRequest.Method.HEAD) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(status, channel.newBuilder()));
        }

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.prettyPrint().lfAtEnd();
            builder.startObject();
            builder.field("ok", status.equals(RestStatus.OK));
            builder.field("status", status.getStatus());

            String nodeName = NODE_NAME_SETTING.get(settings);
            if (nodeName != null && !nodeName.isEmpty()) {
                builder.field("name", nodeName);
            }

            builder.field("cluster_name", clusterName.value());
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
                .field("lucene_version", org.apache.lucene.util.Version.LATEST.toString())
                .endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        };
    }

}

