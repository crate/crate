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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestHandlerAction to return a html page containing informations about crate
 */
public class AdminUIFrontpageAction extends BaseRestHandler {

    @Inject
    public AdminUIFrontpageAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/admin", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        BytesRestResponse resp = new BytesRestResponse(RestStatus.TEMPORARY_REDIRECT);
        resp.addHeader("Location", "/_plugin/crate-admin/");
        channel.sendResponse(resp);
    }

}
