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

import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLRequestBuilder;
import io.crate.action.sql.SQLResponse;
import io.crate.action.sql.parser.SQLXContentSourceContext;
import io.crate.action.sql.parser.SQLXContentSourceParser;
import io.crate.exceptions.SQLParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.PrintWriter;
import java.io.StringWriter;

public class RestSQLAction extends BaseRestHandler {

    @Inject
    public RestSQLAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.POST, "/_sql", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) throws Exception {

        final SQLRequestBuilder requestBuilder = new SQLRequestBuilder(client);
        if (request.hasContent()) {
            SQLXContentSourceContext context = new SQLXContentSourceContext();
            SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
            try {
                parser.parseSource(request.content());
            } catch (SQLParseException e) {
                StringWriter stackTrace = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTrace));
                channel.sendResponse(new CrateThrowableRestResponse(channel,
                        new SQLActionException(e.getMessage(), 4000, RestStatus.BAD_REQUEST, stackTrace.toString())));
            }
            requestBuilder.stmt(context.stmt());
            requestBuilder.args(context.args());
            requestBuilder.includeTypesOnResponse(request.paramAsBoolean("types", false));
        } else {
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                    new SQLActionException("missing request body", 4000, RestStatus.BAD_REQUEST, null)));
        }
        requestBuilder.execute(new ActionListener<SQLResponse>() {

            @Override
            public void onResponse(SQLResponse response) {
                try {
                    XContentBuilder builder = channel.newBuilder();
                    response.toXContent(builder, request);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new CrateThrowableRestResponse(channel, e));
                } catch (Throwable e1) {
                    logger.error("failed to send failure response", e1);
                }
            }
        });
    }
}