/*
 * Licensed to CRATE Technology GmbH ("Crate") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license
 * agreement with Crate these terms will supersede the license
 * and you may use the software solely pursuant to the terms of
 * the relevant commercial agreement.
 */

package io.crate.rest.action.admin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestHandlerAction to return a html page containing information about crate
 */
public class AdminUIFrontpageAction extends BaseRestHandler {

    private final RestController controller;
    private final AdminUIStaticFileRequestFilter requestFilter;

    @Inject
    public AdminUIFrontpageAction(Settings settings,
                                  RestController controller,
                                  AdminUIStaticFileRequestFilter staticFileRequestFilter) {
        super(settings);
        this.controller = controller;
        this.requestFilter = staticFileRequestFilter;
    }

    public void registerHandler() {
        controller.registerHandler(GET, "/admin", this);
        // FIXME replace with a rest handler wrapper - there can be only one plugin that registers a wrapper
        // FIXME (see https://github.com/elastic/elasticsearch/commit/34eb23e98ee3e1c6fac99f88397518e42d4e2020 ) so we'll
        // FIXME have to look into merging this into the CrateRestHandlerWrapper or something simillar
        // FIXME controller.registerFilter(requestFilter);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        BytesRestResponse resp = new BytesRestResponse(RestStatus.TEMPORARY_REDIRECT, "");
        resp.addHeader("Location", "/");
        return channel -> channel.sendResponse(resp);
    }
}
