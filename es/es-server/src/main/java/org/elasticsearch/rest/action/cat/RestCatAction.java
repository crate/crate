/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestCatAction extends BaseRestHandler {

    private static final String CAT = "=^.^=";
    private static final String CAT_NL = CAT + "\n";
    private final String HELP;

    @Inject
    public RestCatAction(Settings settings, RestController controller, List<AbstractCatAction> catActions) {
        super(settings);
        controller.registerHandler(GET, "/_cat", this);
        StringBuilder sb = new StringBuilder();
        sb.append(CAT_NL);
        for (AbstractCatAction catAction : catActions) {
            catAction.documentation(sb);
        }
        HELP = sb.toString();
    }

    @Override
    public String getName() {
        return "cat_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, HELP));
    }

}
