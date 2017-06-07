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

import io.crate.action.sql.*;
import io.crate.action.sql.parser.SQLXContentSourceContext;
import io.crate.action.sql.parser.SQLXContentSourceParser;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccounting;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.SQLParseException;
import io.crate.types.DataType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import static io.crate.action.sql.SQLOperations.Session.UNNAMED;

public class RestSQLAction extends BaseRestHandler {

    private static final String REQUEST_HEADER_USER = "User";
    private static final String REQUEST_HEADER_SCHEMA = "Default-Schema";
    private static final int DEFAULT_SOFT_LIMIT = 10_000;

    private final SQLOperations sqlOperations;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public RestSQLAction(Settings settings,
                         Client client,
                         RestController controller,
                         SQLOperations sqlOperations,
                         CrateCircuitBreakerService breakerService) {
        super(settings, controller, client);
        this.sqlOperations = sqlOperations;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY);

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
        if (args != null && args.length > 0 && bulkArgs != null && bulkArgs.length > 0) {
            channel.sendResponse(new CrateThrowableRestResponse(channel,
                new SQLActionException("request body contains args and bulk_args. It's forbidden to provide both",
                    4000, RestStatus.BAD_REQUEST)));
            return;
        }
        if (bulkArgs != null && bulkArgs.length > 0) {
            executeBulkRequest(context, request, channel);
        } else {
            executeSimpleRequest(context, request, channel);
        }
    }

    private static Set<Option> toOptions(RestRequest request) {
        String user = request.header(REQUEST_HEADER_USER);
        if (user != null && !user.isEmpty() && user.toLowerCase(Locale.ENGLISH).contains("odbc")) {
            return EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT);
        }
        return Option.NONE;
    }

    private void executeSimpleRequest(SQLXContentSourceContext context, final RestRequest request, final RestChannel channel) {
        SQLOperations.Session session = sqlOperations.createSession(
            request.header(REQUEST_HEADER_SCHEMA),
            toOptions(request),
            DEFAULT_SOFT_LIMIT);
        try {
            final long startTime = System.nanoTime();
            session.parse(UNNAMED, context.stmt(), Collections.<DataType>emptyList());
            List<Object> args = context.args() == null ? Collections.emptyList() : Arrays.asList(context.args());
            session.bind(UNNAMED, UNNAMED, args, null);
            List<Field> outputFields = session.describe('P', UNNAMED);
            if (outputFields == null) {
                ResultReceiver resultReceiver
                    = new RestRowCountReceiver(channel, startTime, request.paramAsBoolean("types", false));
                session.execute(UNNAMED, 0, resultReceiver);
            } else {
                ResultReceiver resultReceiver = new RestResultSetReceiver(
                    channel,
                    outputFields,
                    startTime,
                    new RowAccounting(Symbols.extractTypes(outputFields), new RamAccountingContext("http-result", circuitBreaker)),
                    request.paramAsBoolean("types", false)
                );
                session.execute(UNNAMED, 0, resultReceiver);
            }
            session.sync();
        } catch (Throwable t) {
            errorResponse(channel, t);
        }
    }

    private void executeBulkRequest(SQLXContentSourceContext context, final RestRequest request, final RestChannel channel) {
        SQLOperations.Session session = sqlOperations.createSession(
            request.header(REQUEST_HEADER_SCHEMA),
            toOptions(request),
            DEFAULT_SOFT_LIMIT);
        try {
            final long startTime = System.nanoTime();
            session.parse(UNNAMED, context.stmt(), Collections.<DataType>emptyList());
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
            session.sync().whenComplete((Object result, Throwable t) -> {
                if (t == null) {
                    try {
                        XContentBuilder builder = ResultToXContentBuilder.builder(channel)
                            .cols(Collections.<Field>emptyList())
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
        } catch (Throwable t) {
            errorResponse(channel, t);
        }
    }

    private void errorResponse(RestChannel channel, Throwable t) {
        try {
            channel.sendResponse(new CrateThrowableRestResponse(channel, SQLExceptions.createSQLActionException(t)));
        } catch (Throwable e) {
            logger.error("failed to send failure response", e);
        }
    }
}
