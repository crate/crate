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

package io.crate.protocols.http;

import static io.crate.protocols.http.Headers.isAcceptJson;
import static io.crate.protocols.http.Headers.isBrowser;
import static io.crate.protocols.http.Headers.isCloseConnection;
import static io.crate.protocols.http.Responses.contentResponse;
import static io.crate.protocols.http.Responses.redirectTo;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.rest.RestStatus;
import org.jetbrains.annotations.Nullable;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

public class MainAndStaticFileHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final Path sitePath;
    private final NodeClient client;
    private final Netty4CorsConfig corsConfig;
    private final String nodeName;

    public MainAndStaticFileHandler(String nodeName, Path home, NodeClient client, Netty4CorsConfig corsConfig) {
        this.nodeName = nodeName;
        this.sitePath = home.resolve("lib").resolve("site");
        this.client = client;
        this.corsConfig = corsConfig;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        switch (msg.uri().trim().toLowerCase(Locale.ENGLISH)) {
            case "/admin":
            case "/_plugin/crate-admin":
                writeResponse(ctx, msg, redirectTo("/"));
                break;

            case "/index.html":
                writeResponse(ctx, msg, StaticSite.serveSite(sitePath, msg, ctx.alloc()));
                break;

            default:
                serveJsonOrSite(msg, ctx.alloc())
                    .whenComplete((resp, err) -> {
                        if (err == null) {
                            writeResponse(ctx, msg, resp);
                        } else {
                            var errResp = contentResponse(HttpResponseStatus.BAD_REQUEST, ctx.alloc(), err.getMessage());
                            writeResponse(ctx, msg, errResp);
                        }
                    });
                break;
        }
    }

    private void writeResponse(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse resp) {
        Netty4CorsHandler.setCorsResponseHeaders(req, resp, corsConfig);
        ChannelPromise promise = ctx.newPromise();
        if (isCloseConnection(req)) {
            promise.addListener(ChannelFutureListener.CLOSE);
        } else {
            Headers.setKeepAlive(req.protocolVersion(), resp);
        }
        ctx.channel().writeAndFlush(resp, promise);
    }

    private CompletableFuture<FullHttpResponse> serveJsonOrSite(FullHttpRequest request, ByteBufAllocator alloc) throws IOException {
        HttpHeaders headers = request.headers();
        String userAgent = headers.get(HttpHeaderNames.USER_AGENT);
        String accept = headers.get(HttpHeaderNames.ACCEPT);
        if (shouldServeJSON(userAgent, accept, request.uri())) {
            return serveJSON(request.method(), alloc);
        } else {
            return completedFuture(StaticSite.serveSite(sitePath, request, alloc));
        }
    }

    private CompletableFuture<FullHttpResponse> serveJSON(HttpMethod method, ByteBufAllocator alloc) {
        var requestClusterState = new ClusterStateRequest()
            .blocks(true)
            .metadata(false)
            .nodes(false)
            .local(true);
        return client.execute(ClusterStateAction.INSTANCE, requestClusterState)
            .thenApply(resp -> clusterStateRespToHttpResponse(method, resp, alloc, nodeName));
    }

    private static FullHttpResponse clusterStateRespToHttpResponse(HttpMethod method,
                                                                   ClusterStateResponse response,
                                                                   ByteBufAllocator alloc,
                                                                   @Nullable String nodeName) {
        var httpStatus = response.getState().blocks().hasGlobalBlockWithStatus(RestStatus.SERVICE_UNAVAILABLE)
            ? HttpResponseStatus.SERVICE_UNAVAILABLE
            : HttpResponseStatus.OK;
        try {
            DefaultFullHttpResponse resp;
            if (method == HttpMethod.HEAD) {
                resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, httpStatus);
                HttpUtil.setContentLength(resp, 0);
            } else {
                var buffer = alloc.buffer();
                try (var outputStream = new ByteBufOutputStream(buffer)) {
                    writeJSON(outputStream, response, httpStatus, nodeName);
                }
                resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, httpStatus, buffer);
                resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
                HttpUtil.setContentLength(resp, buffer.readableBytes());
            }
            return resp;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeJSON(OutputStream outputStream,
                                  ClusterStateResponse response,
                                  HttpResponseStatus status,
                                  @Nullable String nodeName) throws IOException {
        var builder = new XContentBuilder(JsonXContent.JSON_XCONTENT, outputStream);
        builder.prettyPrint().lfAtEnd();
        builder.startObject();
        builder.field("ok", status == HttpResponseStatus.OK);
        builder.field("status", HttpResponseStatus.OK.code());
        if (nodeName != null && !nodeName.isEmpty()) {
            builder.field("name", nodeName);
        }
        builder.field("cluster_name", response.getClusterName().value());
        builder.startObject("version")
            .field("number", Version.CURRENT.externalNumber())
            .field("build_hash", Build.CURRENT.hash())
            .field("build_timestamp", Build.CURRENT.timestamp())
            .field("build_snapshot", Version.CURRENT.isSnapshot())
            .field("lucene_version", org.apache.lucene.util.Version.LATEST.toString())
            .endObject();
        builder.endObject();
        builder.flush();
        builder.close();
    }

    private static boolean shouldServeJSON(String userAgent, String accept, String uri) {
        boolean isRoot = uri.equals("/");
        boolean forceJson = isAcceptJson(accept);
        return isRoot && (forceJson || !isBrowser(userAgent));
    }
}
