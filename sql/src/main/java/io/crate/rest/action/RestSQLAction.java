/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.rest.action;

import com.google.common.collect.ImmutableSet;
import io.crate.action.sql.*;
import io.crate.action.sql.parser.SQLXContentSourceContext;
import io.crate.action.sql.parser.SQLXContentSourceParser;
import io.crate.analyze.symbol.Field;
import io.crate.exceptions.SQLParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import static io.crate.action.sql.SQLOperations.Session.UNNAMED;

public class RestSQLAction extends BaseRestHandler {

    private static final String REQUEST_HEADER_USER = "User";
    private static final String REQUEST_HEADER_SCHEMA = "Default-Schema";
    private static final int DEFAULT_SOFT_LIMIT = 10_000;

    private final SQLOperations sqlOperations;

    @Inject
    public RestSQLAction(Settings settings, RestController controller, SQLOperations sqlOperations) {
        super(settings);
        this.sqlOperations = sqlOperations;

        controller.registerHandler(RestRequest.Method.POST, "/_sql", this);
    }

    private static void sendBadRequest(RestChannel channel, String errorMsg) throws IOException {
        channel.sendResponse(new CrateThrowableRestResponse(
            channel,
            new SQLActionException(errorMsg, 4000, RestStatus.BAD_REQUEST)
        ));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!request.hasContent()) {
            return channel -> sendBadRequest(channel, "missing request body");
        }

        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser parser = new SQLXContentSourceParser(context);
        try {
            parser.parseSource(request.content());
        } catch (SQLParseException e) {
            StringWriter stackTrace = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTrace));
            return channel -> channel.sendResponse(
                new CrateThrowableRestResponse(
                    channel,
                    new SQLActionException(e.getMessage(), 4000, RestStatus.BAD_REQUEST, e.getStackTrace()))
            );
        }

        Object[] args = context.args();
        Object[][] bulkArgs = context.bulkArgs();
        if (args != null && args.length > 0 && bulkArgs != null && bulkArgs.length > 0) {
            return channel -> sendBadRequest(channel, "request body contains args and bulk_args. It's forbidden to provide both");
        }
        if (bulkArgs != null && bulkArgs.length > 0) {
            return executeBulkRequest(context, request);
        } else {
            return executeSimpleRequest(context, request);
        }
    }

    @Override
    protected Set<String> responseParams() {
        return ImmutableSet.of("types");
    }

    private static Set<Option> toOptions(RestRequest request) {
        String user = request.header(REQUEST_HEADER_USER);
        if (user != null && !user.isEmpty() && user.toLowerCase(Locale.ENGLISH).contains("odbc")) {
            return EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT);
        }
        return Option.NONE;
    }

    private RestChannelConsumer executeSimpleRequest(SQLXContentSourceContext context, final RestRequest request) {
        SQLOperations.Session session = sqlOperations.createSession(
            request.header(REQUEST_HEADER_SCHEMA),
            null,
            toOptions(request),
            DEFAULT_SOFT_LIMIT);
        try {
            final long startTime = System.nanoTime();
            session.parse(UNNAMED, context.stmt(), Collections.emptyList());
            List<Object> args = context.args() == null ? Collections.emptyList() : Arrays.asList(context.args());
            session.bind(UNNAMED, UNNAMED, args, null);
            List<Field> outputFields = session.describe('P', UNNAMED);
            if (outputFields == null) {
                return channel -> {
                    try {
                        ResultReceiver resultReceiver
                            = new RestRowCountReceiver(channel, startTime, request.paramAsBoolean("types", false));
                        session.execute(UNNAMED, 0, resultReceiver);
                        session.sync();
                    } catch (Throwable t) {
                        errorResponse(channel, t);
                    }
                };
            }
            return channel -> {
                try {
                    ResultReceiver resultReceiver =
                        new RestResultSetReceiver(channel, outputFields, startTime, request.paramAsBoolean("types", false));
                    session.execute(UNNAMED, 0, resultReceiver);
                    session.sync();
                } catch (Throwable t) {
                    errorResponse(channel, t);
                }
            };
        } catch (Throwable t) {
            return channel -> errorResponse(channel, t);
        }
    }

    private RestChannelConsumer executeBulkRequest(SQLXContentSourceContext context, final RestRequest request) {
        SQLOperations.Session session = sqlOperations.createSession(
            request.header(REQUEST_HEADER_SCHEMA),
            null,
            toOptions(request),
            DEFAULT_SOFT_LIMIT);
        try {
            final long startTime = System.nanoTime();
            session.parse(UNNAMED, context.stmt(), Collections.emptyList());
            Object[][] bulkArgs = context.bulkArgs();
            final RestBulkRowCountReceiver.Result[] results = new RestBulkRowCountReceiver.Result[bulkArgs.length];
            if (results.length == 0) {
                session.bind(UNNAMED, UNNAMED, Collections.emptyList(), null);
                session.execute(UNNAMED, 0, new BaseResultReceiver());
            } else {
                for (int i = 0; i < bulkArgs.length; i++) {
                    session.bind(UNNAMED, UNNAMED, Arrays.asList(bulkArgs[i]), null);
                    ResultReceiver resultReceiver = new RestBulkRowCountReceiver(results, i);
                    session.execute(UNNAMED, 0, resultReceiver);
                }
            }
            List<Field> outputColumns = session.describe('P', UNNAMED);
            if (outputColumns != null) {
                throw new UnsupportedOperationException(
                    "Bulk operations for statements that return result sets is not supported");
            }
            return channel -> {
                session.sync().whenComplete((Object result, Throwable t) -> {
                    if (t == null) {
                        try {
                            XContentBuilder builder = ResultToXContentBuilder.builder(channel)
                                .cols(Collections.emptyList())
                                .duration(startTime)
                                .bulkRows(results).build();
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                        } catch (Throwable e) {
                            errorResponse(channel, e);
                        }
                    } else {
                        errorResponse(channel, t);
                    }
                });
            };
        } catch (Throwable t) {
            return channel -> errorResponse(channel, t);
        }
    }

    private void errorResponse(RestChannel channel, Throwable t) {
        try {
            channel.sendResponse(new CrateThrowableRestResponse(channel, t));
        } catch (Throwable e) {
            logger.error("failed to send failure response", e);
        }
    }
}
