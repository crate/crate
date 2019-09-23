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

import io.crate.action.sql.DescribeResult;
import io.crate.action.sql.Option;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.action.sql.SessionContext;
import io.crate.action.sql.parser.SQLRequestParseContext;
import io.crate.action.sql.parser.SQLRequestParser;
import io.crate.auth.AuthSettings;
import io.crate.auth.user.AccessControl;
import io.crate.auth.user.User;
import io.crate.auth.user.UserLookup;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.exceptions.SQLExceptions;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbols;
import io.crate.protocols.http.Headers;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.transport.netty4.Netty4Utils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.crate.action.sql.Session.UNNAMED;
import static io.crate.protocols.http.Headers.isCloseConnection;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class SqlHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOGGER = LogManager.getLogger(SqlHttpHandler.class);
    private static final String REQUEST_HEADER_USER = "User";
    private static final String REQUEST_HEADER_SCHEMA = "Default-Schema";

    private final Settings settings;
    private final SQLOperations sqlOperations;
    private final Function<String, CircuitBreaker> circuitBreakerProvider;
    private final UserLookup userLookup;
    private final Function<SessionContext, AccessControl> getAccessControl;
    private final Netty4CorsConfig corsConfig;

    private Session session;

    SqlHttpHandler(Settings settings,
                   SQLOperations sqlOperations,
                   Function<String, CircuitBreaker> circuitBreakerProvider,
                   UserLookup userLookup,
                   Function<SessionContext, AccessControl> getAccessControl,
                   Netty4CorsConfig corsConfig) {
        super(false);
        this.settings = settings;
        this.sqlOperations = sqlOperations;
        this.circuitBreakerProvider = circuitBreakerProvider;
        this.userLookup = userLookup;
        this.getAccessControl = getAccessControl;
        this.corsConfig = corsConfig;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (request.uri().startsWith("/_sql")) {
            Session session = ensureSession(request);
            Map<String, List<String>> parameters = new QueryStringDecoder(request.uri()).parameters();
            ByteBuf content = request.content();
            handleSQLRequest(session, content, paramContainFlag(parameters, "types"))
                .whenComplete((result, t) -> {
                    try {
                        sendResponse(session, ctx, request, parameters, result, t);
                    } catch (Throwable ex) {
                        LOGGER.error("Error sending response", ex);
                        throw ex;
                    } finally {
                        request.release();
                    }
                });
        } else {
            ctx.fireChannelRead(request);
        }
    }

    /**
     * @return true if the parameters contains a flag entry (e.g. "/_sql?flag" or "/_sql?flag=true")
     */
    private static boolean paramContainFlag(Map<String, List<String>> parameters, String flag) {
        List<String> values = parameters.get(flag);
        return values != null && (values.equals(singletonList("")) || values.equals(singletonList("true")));
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (session != null) {
            session.close();
            session = null;
        }
        super.channelUnregistered(ctx);
    }

    private void sendResponse(Session session,
                              ChannelHandlerContext ctx,
                              FullHttpRequest request,
                              Map<String, List<String>> parameters,
                              XContentBuilder result,
                              @Nullable Throwable t) {
        final HttpVersion httpVersion = request.protocolVersion();
        final DefaultFullHttpResponse resp;
        final ByteBuf content;
        if (t == null) {
            content = Netty4Utils.toByteBuf(BytesReference.bytes(result));
            resp = new DefaultFullHttpResponse(httpVersion, HttpResponseStatus.OK, content);
            resp.headers().add(HttpHeaderNames.CONTENT_TYPE, result.contentType().mediaType());
        } else {
            SQLActionException sqlActionException = SQLExceptions.forWireTransmission(
                getAccessControl.apply(session.sessionContext()), t);
            String mediaType;
            boolean includeErrorTrace = paramContainFlag(parameters, "error_trace");
            try (XContentBuilder contentBuilder = HTTPErrorFormatter.convert(sqlActionException, includeErrorTrace)) {
                content = Netty4Utils.toByteBuf(BytesReference.bytes(contentBuilder));
                mediaType = contentBuilder.contentType().mediaType();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            resp = new DefaultFullHttpResponse(httpVersion, sqlActionException.status(), content);
            resp.headers().add(HttpHeaderNames.CONTENT_TYPE, mediaType);
        }
        Netty4CorsHandler.setCorsResponseHeaders(request, resp, corsConfig);
        resp.headers().add(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(content.readableBytes()));
        boolean closeConnection = isCloseConnection(request);
        ChannelPromise promise = ctx.newPromise();
        if (closeConnection) {
            promise.addListener(ChannelFutureListener.CLOSE);
        } else {
            Headers.setKeepAlive(httpVersion, resp);
        }
        ctx.writeAndFlush(resp, promise);
    }

    private CompletableFuture<XContentBuilder> handleSQLRequest(Session session, ByteBuf content, boolean includeTypes) {
        SQLRequestParseContext parseContext;
        try {
            parseContext = SQLRequestParser.parseSource(Netty4Utils.toBytesReference(content));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
        Object[] args = parseContext.args();
        Object[][] bulkArgs = parseContext.bulkArgs();
        if (bothProvided(args, bulkArgs)) {
            return CompletableFuture.failedFuture(new SQLActionException(
                "request body contains args and bulk_args. It's forbidden to provide both", 4000, HttpResponseStatus.BAD_REQUEST));
        }
        try {
            if (args != null || bulkArgs == null) {
                return executeSimpleRequest(session, parseContext.stmt(), args, includeTypes);
            } else {
                return executeBulkRequest(session, parseContext.stmt(), bulkArgs);
            }
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private Session ensureSession(FullHttpRequest request) {
        String defaultSchema = request.headers().get(REQUEST_HEADER_SCHEMA);
        User user = userFromAuthHeader(request.headers().get(HttpHeaderNames.AUTHORIZATION));
        Set<Option> options = optionsFromUserHeader(request.headers().get(REQUEST_HEADER_USER));
        Session session = this.session;
        if (session == null) {
            session = sqlOperations.createSession(defaultSchema, user, options);
        } else if (optionsChanged(user, options, session.sessionContext())) {
            session.close();
            session = sqlOperations.createSession(defaultSchema, user, options);
        } else {
            // We don't want to keep "set session" settings across requests yet to not mess with clients doing
            // per request round-robin
            SessionContext sessionContext = session.sessionContext();
            sessionContext.resetToDefaults();
            if (defaultSchema != null) {
                sessionContext.setSearchPath(defaultSchema);
            }
        }
        this.session = session;
        return session;
    }

    private static boolean optionsChanged(User user, Set<Option> options, SessionContext sessionContext) {
        return !sessionContext.user().equals(user) || !sessionContext.options().equals(options);
    }

    private CompletableFuture<XContentBuilder> executeSimpleRequest(Session session,
                                                                    String stmt,
                                                                    Object[] args,
                                                                    boolean includeTypes) throws IOException {
        long startTimeInNs = System.nanoTime();
        session.parse(UNNAMED, stmt, emptyList());
        session.bind(UNNAMED, UNNAMED, args == null ? emptyList() : asList(args), null);
        DescribeResult description = session.describe('P', UNNAMED);
        List<Field> resultFields = description.getFields();
        ResultReceiver<XContentBuilder> resultReceiver;
        if (resultFields == null) {
            resultReceiver = new RestRowCountReceiver(JsonXContent.contentBuilder(), startTimeInNs, includeTypes);
        } else {
            resultReceiver = new RestResultSetReceiver(
                JsonXContent.contentBuilder(),
                resultFields,
                startTimeInNs,
                new RowAccountingWithEstimators(
                    Symbols.typeView(resultFields),
                    new RamAccountingContext("http-result", circuitBreakerProvider.apply(CrateCircuitBreakerService.QUERY))
                ),
                includeTypes
            );
        }
        session.execute(UNNAMED, 0, resultReceiver);
        return session.sync()
            .thenCompose(ignored -> resultReceiver.completionFuture());
    }

    private CompletableFuture<XContentBuilder> executeBulkRequest(Session session,
                                                                  String stmt,
                                                                  Object[][] bulkArgs) {
        final long startTimeInNs = System.nanoTime();
        session.parse(UNNAMED, stmt, emptyList());
        final RestBulkRowCountReceiver.Result[] results = new RestBulkRowCountReceiver.Result[bulkArgs.length];
        for (int i = 0; i < bulkArgs.length; i++) {
            session.bind(UNNAMED, UNNAMED, Arrays.asList(bulkArgs[i]), null);
            ResultReceiver resultReceiver = new RestBulkRowCountReceiver(results, i);
            session.execute(UNNAMED, 0, resultReceiver);
        }
        if (results.length > 0) {
            DescribeResult describeResult = session.describe('P', UNNAMED);
            if (describeResult.getFields() != null) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException(
                            "Bulk operations for statements that return result sets is not supported"));
            }
        }
        return session.sync()
            .thenApply(ignored -> {
                try {
                    return ResultToXContentBuilder.builder(JsonXContent.contentBuilder())
                        .cols(emptyList())
                        .duration(startTimeInNs)
                        .bulkRows(results)
                        .build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    private static Set<Option> optionsFromUserHeader(String user) {
        if (user != null && !user.isEmpty() && user.toLowerCase(Locale.ENGLISH).contains("odbc")) {
            return EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT);
        }
        return Option.NONE;
    }

    User userFromAuthHeader(@Nullable String authHeaderValue) {
        String username = Headers.extractCredentialsFromHttpBasicAuthHeader(authHeaderValue).v1();
        // Fallback to trusted user from configuration
        if (username == null || username.isEmpty()) {
            username = AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.setting().get(settings);
        }
        return userLookup.findUser(username);
    }

    private static boolean bothProvided(@Nullable Object[] args, @Nullable Object[][] bulkArgs) {
        return args != null && args.length > 0 && bulkArgs != null && bulkArgs.length > 0;
    }
}
