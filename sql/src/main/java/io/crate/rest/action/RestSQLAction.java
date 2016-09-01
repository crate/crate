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

import io.crate.action.sql.*;
import io.crate.action.sql.parser.SQLXContentSourceContext;
import io.crate.action.sql.parser.SQLXContentSourceParser;
import io.crate.exceptions.SQLParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;

public class RestSQLAction extends BaseRestHandler {

    private static final String REQUEST_HEADER_USER = "User";
    private static final String REQUEST_HEADER_SCHEMA = "Default-Schema";

    @Inject
    public RestSQLAction(Settings settings, Client client, RestController controller) {
        super(settings, controller, client);

        controller.registerHandler(RestRequest.Method.POST, "/_sql", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) throws Exception {
        if (!request.hasContent()) {
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                    new SQLActionException("missing request body", 4000, RestStatus.BAD_REQUEST)));
            return;
        }

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        try {
            parser.parseSource(request.content());
        } catch (SQLParseException e) {
            StringWriter stackTrace = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTrace));
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                    new SQLActionException(e.getMessage(), 4000, RestStatus.BAD_REQUEST, e.getStackTrace())));
            return;
        }

        Object[] args = context.args();
        Object[][] bulkArgs = context.bulkArgs();
        if(args != null && args.length > 0 && bulkArgs != null && bulkArgs.length > 0){
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                    new SQLActionException("request body contains args and bulk_args. It's forbidden to provide both",
                            4000, RestStatus.BAD_REQUEST)));
            return;
        }
        if (bulkArgs != null && bulkArgs.length > 0) {
            executeBulkRequest(context, request, channel, client);
        } else {
            executeSimpleRequest(context, request, channel, client);
        }
    }

    private static void addFlags(SQLRequestBuilder requestBuilder, RestRequest request) {
        String user = request.header(REQUEST_HEADER_USER);
        if (isOdbc(user)) {
            requestBuilder.addFlagsToRequestHeader(SQLBaseRequest.HEADER_FLAG_ALLOW_QUOTED_SUBSCRIPT);
        }
    }

    private static void addFlags(SQLBulkRequestBuilder requestBuilder, RestRequest request) {
        String user = request.header(REQUEST_HEADER_USER);
        if (isOdbc(user)) {
            requestBuilder.addFlagsToRequestHeader(SQLBaseRequest.HEADER_FLAG_ALLOW_QUOTED_SUBSCRIPT);
        }
    }

    private static boolean isOdbc(String user) {
        return user != null && !user.isEmpty() && user.toLowerCase(Locale.ENGLISH).contains("odbc");
    }

    private void executeSimpleRequest(SQLXContentSourceContext context, final RestRequest request, final RestChannel channel, Client client) {
        final SQLRequestBuilder requestBuilder = new SQLRequestBuilder(client, SQLAction.INSTANCE);
        requestBuilder.stmt(context.stmt());
        requestBuilder.args(context.args());
        requestBuilder.includeTypesOnResponse(request.paramAsBoolean("types", false));
        addFlags(requestBuilder, request);
        requestBuilder.setSchema(request.header(REQUEST_HEADER_SCHEMA));
        requestBuilder.execute(RestSQLAction.<SQLResponse>newListener(request, channel));
    }

    private void executeBulkRequest(SQLXContentSourceContext context, RestRequest request, RestChannel channel, Client client) {
        final SQLBulkRequestBuilder requestBuilder = new SQLBulkRequestBuilder(client, SQLBulkAction.INSTANCE);
        requestBuilder.stmt(context.stmt());
        requestBuilder.bulkArgs(context.bulkArgs());
        requestBuilder.includeTypesOnResponse(request.paramAsBoolean("types", false));
        addFlags(requestBuilder, request);
        requestBuilder.setSchema(request.header(REQUEST_HEADER_SCHEMA));
        requestBuilder.execute(RestSQLAction.<SQLBulkResponse>newListener(request, channel));
    }

    private static <TResponse extends SQLBaseResponse> ActionListener<TResponse> newListener(
            RestRequest request, RestChannel channel) {
        return new SQLResponseListener<>(request, channel);
    }

    private static class SQLResponseListener<TResponse extends SQLBaseResponse> implements ActionListener<TResponse> {

        private static final ESLogger logger = Loggers.getLogger(SQLResponseListener.class);
        private final RestRequest request;
        private final RestChannel channel;

        public SQLResponseListener(RestRequest request, RestChannel channel) {
            this.request = request;
            this.channel = channel;
        }

        @Override
        public void onResponse(TResponse tResponse) {
            try {
                XContentBuilder builder = channel.newBuilder();
                tResponse.toXContent(builder, request);
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
    }
}
