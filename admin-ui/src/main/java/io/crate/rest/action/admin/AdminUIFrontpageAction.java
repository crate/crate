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

import io.crate.rest.CrateRestMainAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestHandlerAction to return a html page containing information about crate
 */
public class AdminUIFrontpageAction extends BaseRestHandler {

    static final String ADMIN_ENDPOINT = "/admin/";
    private static final Pattern USER_AGENT_BROWSER_PATTERN = Pattern.compile("(Mozilla|Chrome|Safari|Opera|Android|AppleWebKit)+?[/\\s][\\d.]+");
    private final CrateRestMainAction crateRestMainAction;
    private final RestController controller;
    private final AdminUIStaticFileRequestFilter requestFilter;

    @Inject
    public AdminUIFrontpageAction(CrateRestMainAction crateRestMainAction, Settings settings, RestController controller, AdminUIStaticFileRequestFilter staticFileRequestFilter) {
        super(settings);
        this.crateRestMainAction = crateRestMainAction;
        this.controller = controller;
        this.requestFilter = staticFileRequestFilter;
    }

    public void registerHandler() {
        controller.registerHandler(GET, "/", this);
        controller.registerHandler(GET, "/admin", this);
        controller.registerFilter(requestFilter);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!isBrowser(request.header("user-agent"))
            || (request.header("accept") != null && request.header("accept").contains("application/json"))) {
            return crateRestMainAction.prepareRequest(request, client);
        }

        if (request.header("accept") != null && request.header("accept").contains("application/json")){
            return crateRestMainAction.prepareRequest(request, client);
        }

        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            BytesRestResponse resp = new BytesRestResponse(RestStatus.TEMPORARY_REDIRECT, builder);
            resp.addHeader("Location", ADMIN_ENDPOINT);
            channel.sendResponse(resp);
        };
    }

    private boolean isBrowser(String headerValue) {
        if (headerValue == null){
            return false;
        }
        String engine = headerValue.split("\\s+")[0];
        Matcher matcher = USER_AGENT_BROWSER_PATTERN.matcher(engine);

        return matcher.matches();
    }
}
