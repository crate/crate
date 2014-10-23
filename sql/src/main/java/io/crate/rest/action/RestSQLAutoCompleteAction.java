/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.action.sql.parser.SQLXContentSourceContext;
import io.crate.action.sql.parser.SQLXContentSourceParser;
import io.crate.autocomplete.SQLAutoCompleteRequest;
import io.crate.autocomplete.SQLAutoCompleteRequestBuilder;
import io.crate.autocomplete.SQLAutoCompleteResponse;
import io.crate.exceptions.SQLParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.PrintWriter;
import java.io.StringWriter;

public class RestSQLAutoCompleteAction extends BaseRestHandler {

    @Inject
    public RestSQLAutoCompleteAction(Settings settings,
                                     Client client,
                                     RestController restController) {
        super(settings, client);
        restController.registerHandler(RestRequest.Method.POST, "_sql_complete", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, Client client) throws Exception {
        if (!request.hasContent()) {
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                    new SQLActionException("missing request body", 4000, RestStatus.BAD_REQUEST, null)));
        }

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        try {
            parser.parseSource(request.content());
        } catch (SQLParseException e) {
            StringWriter stackTrace = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTrace));
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                    new SQLActionException(e.getMessage(), 4000, RestStatus.BAD_REQUEST, stackTrace.toString())));
            return;
        }

        final SQLAutoCompleteRequestBuilder builder =
                new SQLAutoCompleteRequestBuilder(client, new SQLAutoCompleteRequest(context.stmt()));

        builder.execute(new ActionListener<SQLAutoCompleteResponse>() {
            @Override
            public void onResponse(SQLAutoCompleteResponse sqlAutoCompleteResponse) {
                try {
                    XContentBuilder xContentBuilder = channel.newBuilder();
                    sqlAutoCompleteResponse.toXContent(xContentBuilder, request);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, xContentBuilder));
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
