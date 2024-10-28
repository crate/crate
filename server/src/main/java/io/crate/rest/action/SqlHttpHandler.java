/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static io.crate.auth.AuthSettings.AUTH_HOST_BASED_JWT_ISS_SETTING;
import static io.crate.data.breaker.BlockBasedRamAccounting.MAX_BLOCK_SIZE_IN_BYTES;
import static io.crate.protocols.SSL.getSession;
import static io.crate.protocols.http.Headers.isCloseConnection;
import static io.crate.session.Session.UNNAMED;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.auth.AccessControl;
import io.crate.auth.AuthSettings;
import io.crate.auth.Credentials;
import io.crate.auth.HttpAuthUpstreamHandler;
import io.crate.auth.Protocol;
import io.crate.breaker.TypedRowAccounting;
import io.crate.common.collections.Lists;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.exceptions.SQLExceptions;
import io.crate.expression.symbol.Symbol;
import io.crate.protocols.http.Headers;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.session.DescribeResult;
import io.crate.session.ResultReceiver;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.crate.session.parser.SQLRequestParseContext;
import io.crate.session.parser.SQLRequestParser;
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

public class SqlHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOGGER = LogManager.getLogger(SqlHttpHandler.class);
    private static final String REQUEST_HEADER_SCHEMA = "Default-Schema";

    private final Settings settings;
    private final Sessions sessions;
    private final Function<String, CircuitBreaker> circuitBreakerProvider;
    private final Roles roles;
    private final Netty4CorsConfig corsConfig;
    private final boolean checkJwtProperties;

    private Session session;

    public SqlHttpHandler(Settings settings,
                          Sessions sessions,
                          Function<String, CircuitBreaker> circuitBreakerProvider,
                          Roles roles,
                          Netty4CorsConfig corsConfig) {
        super(false);
        this.settings = settings;
        this.sessions = sessions;
        this.circuitBreakerProvider = circuitBreakerProvider;
        this.roles = roles;
        this.corsConfig = corsConfig;
        this.checkJwtProperties = settings.get(AUTH_HOST_BASED_JWT_ISS_SETTING.getKey()) == null;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (request.uri().startsWith("/_sql")) {
            Session session = ensureSession(
                new ConnectionProperties(
                    null, // not used
                    Netty4HttpServerTransport.getRemoteAddress(ctx.channel()),
                    Protocol.HTTP,
                    getSession(ctx.channel())),
                request);
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
            var sessionSettings = session.sessionSettings();
            AccessControl accessControl = roles.getAccessControl(sessionSettings.authenticatedUser(), sessionSettings.sessionUser());
            var throwable = SQLExceptions.prepareForClientTransmission(accessControl, t);
            HttpError httpError = HttpError.fromThrowable(throwable);
            String mediaType;
            boolean includeErrorTrace = paramContainFlag(parameters, "error_trace");
            try (XContentBuilder contentBuilder = httpError.toXContent(includeErrorTrace)) {
                content = Netty4Utils.toByteBuf(BytesReference.bytes(contentBuilder));
                mediaType = contentBuilder.contentType().mediaType();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            resp = new DefaultFullHttpResponse(
                httpVersion,
                httpError.httpResponseStatus(),
                content
            );
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
        List<Object> args = parseContext.args();
        List<List<Object>> bulkArgs = parseContext.bulkArgs();
        if (bothProvided(args, bulkArgs)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "request body contains args and bulk_args. It's forbidden to provide both"));
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

    @VisibleForTesting
    Session ensureSession(ConnectionProperties connectionProperties, FullHttpRequest request) {
        String defaultSchema = request.headers().get(REQUEST_HEADER_SCHEMA);
        Role authenticatedUser = userFromAuthHeader(request.headers().get(HttpHeaderNames.AUTHORIZATION));
        Session session = this.session;
        if (session == null) {
            session = sessions.newSession(connectionProperties, defaultSchema, authenticatedUser);
        } else if (session.sessionSettings().authenticatedUser().equals(authenticatedUser) == false) {
            session.close();
            session = sessions.newSession(connectionProperties, defaultSchema, authenticatedUser);
        }
        this.session = session;
        return session;
    }

    private CompletableFuture<XContentBuilder> executeSimpleRequest(Session session,
                                                                    String stmt,
                                                                    List<Object> args,
                                                                    boolean includeTypes) throws IOException {
        long startTimeInNs = System.nanoTime();
        session.parse(UNNAMED, stmt, emptyList());
        session.bind(UNNAMED, UNNAMED, args == null ? emptyList() : args, null);
        DescribeResult description = session.describe('P', UNNAMED);
        List<Symbol> resultFields = description.getFields();
        ResultReceiver<XContentBuilder> resultReceiver;
        if (resultFields == null) {
            resultReceiver = new RestRowCountReceiver(JsonXContent.builder(), startTimeInNs, includeTypes);
        } else {
            CircuitBreaker breaker = circuitBreakerProvider.apply(HierarchyCircuitBreakerService.QUERY);
            RamAccounting ramAccounting = new BlockBasedRamAccounting(
                b -> breaker.addEstimateBytesAndMaybeBreak(b, "http-result"),
                MAX_BLOCK_SIZE_IN_BYTES);
            resultReceiver = new RestResultSetReceiver(
                JsonXContent.builder(),
                resultFields,
                startTimeInNs,
                new TypedRowAccounting(
                    Lists.map(resultFields, Symbol::valueType),
                    ramAccounting
                ),
                includeTypes
            );
            resultReceiver.completionFuture().whenComplete((result, error) -> ramAccounting.close());
        }
        session.execute(UNNAMED, 0, resultReceiver);
        return session.sync()
            .thenCompose(ignored -> resultReceiver.completionFuture());
    }

    private CompletableFuture<XContentBuilder> executeBulkRequest(Session session,
                                                                  String stmt,
                                                                  List<List<Object>> bulkArgs) {
        final long startTimeInNs = System.nanoTime();
        session.parse(UNNAMED, stmt, emptyList());
        final RestBulkRowCountReceiver.Result[] results = new RestBulkRowCountReceiver.Result[bulkArgs.size()];
        for (int i = 0; i < bulkArgs.size(); i++) {
            session.bind(UNNAMED, UNNAMED, bulkArgs.get(i), null);
            var resultReceiver = new RestBulkRowCountReceiver(results, i);
            session.execute(UNNAMED, 0, resultReceiver);
        }
        if (results.length > 0) {
            DescribeResult describeResult = session.describe('P', UNNAMED);
            if (describeResult.getFields() != null) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException(
                            "Bulk operations for statements that return result sets is not supported"));
            }
        }
        var sessionSettings = session.sessionSettings();
        AccessControl accessControl = roles.getAccessControl(sessionSettings.authenticatedUser(), sessionSettings.sessionUser());
        return session.sync()
            .thenApply(ignored -> {
                try {
                    return ResultToXContentBuilder.builder(JsonXContent.builder())
                        .cols(emptyList())
                        .duration(startTimeInNs)
                        .bulkRows(results, accessControl)
                        .build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    /**
     * Doesn't do authentication as it's already done
     * in {@link HttpAuthUpstreamHandler} which is registered before this handler
     * Checks user existence and if not possible to resolve from header (basic or jwt),
     * returns trusted user from configuration.
     */
    Role userFromAuthHeader(@Nullable String authHeaderValue) {
        try (Credentials credentials = Headers.extractCredentialsFromHttpAuthHeader(authHeaderValue)) {
            Predicate<Role> rolePredicate = credentials.matchByToken(checkJwtProperties);
            if (rolePredicate != null) {
                Role role = roles.findUser(rolePredicate);
                if (role != null) {
                    credentials.setUsername(role.name());
                }
            }
            String username = credentials.username();
            // Fallback to trusted user from configuration
            if (username == null || username.isEmpty()) {
                username = AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.get(settings);
            }
            return roles.findUser(username);
        }
    }

    private static boolean bothProvided(@Nullable List<Object> args, @Nullable List<List<Object>> bulkArgs) {
        return args != null && !args.isEmpty() && bulkArgs != null && !bulkArgs.isEmpty();
    }
}
