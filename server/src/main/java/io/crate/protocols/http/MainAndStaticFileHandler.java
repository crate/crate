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
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.state.TransportClusterState;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.jetbrains.annotations.Nullable;

import io.crate.rest.action.HttpErrorStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
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
import io.netty.handler.ssl.NotSslRecordException;

public class MainAndStaticFileHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOGGER = LogManager.getLogger(MainAndStaticFileHandler.class);

    private final Path sitePath;
    private final NodeClient client;
    private final String nodeName;

    public MainAndStaticFileHandler(String nodeName, Path home, NodeClient client) {
        this.nodeName = nodeName;
        this.sitePath = home.resolve("lib").resolve("site");
        this.client = client;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String message = cause.getMessage();
        if (message == null) {
            message = cause.getClass().getSimpleName();
        }
        switch (cause) {
            case ClosedChannelException _ -> LOGGER.warn("Channel closed", cause);
            case NotSslRecordException _ -> {
                // Raised when clients try to send unencrypted data over an encrypted channel
                // This can happen when old instances of the Admin UI are running because the
                // ports of HTTP/HTTPS are the same.
                LOGGER.debug("Received unencrypted message from '{}'", ctx.channel().remoteAddress());
                ctx.channel()
                    .writeAndFlush(new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1,
                        HttpResponseStatus.BAD_REQUEST
                    ))
                    .addListener(ChannelFutureListener.CLOSE);
            }
            case EsRejectedExecutionException _ -> {
                ctx.channel()
                    .writeAndFlush(new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1,
                        HttpResponseStatus.TOO_MANY_REQUESTS
                    ))
                    .addListener(ChannelFutureListener.CLOSE);
            }
            case IOException _ -> {
                if (message.contains("Connection reset")) {
                    LOGGER.debug(message);
                } else {
                    LOGGER.warn(message, cause);
                    send500(ctx, message);
                }
            }
            default -> send500(ctx, message);
        }
    }

    private void send500(ChannelHandlerContext ctx, String message) {
        ByteBuf content = ByteBufUtil.writeUtf8(ctx.alloc(), message);
        var response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.INTERNAL_SERVER_ERROR,
            content
        );
        HttpUtil.setContentLength(response, message.length());
        ctx.channel()
            .writeAndFlush(response)
            .addListener(ChannelFutureListener.CLOSE);
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
        return client.execute(TransportClusterState.ACTION, requestClusterState)
            .thenApply(resp -> clusterStateRespToHttpResponse(method, resp, alloc, nodeName));
    }

    private static FullHttpResponse clusterStateRespToHttpResponse(HttpMethod method,
                                                                   ClusterStateResponse response,
                                                                   ByteBufAllocator alloc,
                                                                   @Nullable String nodeName) {
        ClusterBlocks blocks = response.getState().blocks();
        var httpStatus = (blocks.hasGlobalBlockWithStatus(HttpErrorStatus.SERVICE_UNAVAILABLE)
             || blocks.hasGlobalBlockWithStatus(HttpErrorStatus.MASTER_NOT_DISCOVERED))
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
        builder.field("status", status.code());
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
