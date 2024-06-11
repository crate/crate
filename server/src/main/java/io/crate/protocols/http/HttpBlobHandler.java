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

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.PARTIAL_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.TEMPORARY_REDIRECT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.index.IndexNotFoundException;
import org.jetbrains.annotations.Nullable;

import io.crate.blob.BlobService;
import io.crate.blob.RemoteDigestBlob;
import io.crate.blob.exceptions.DigestMismatchException;
import io.crate.blob.exceptions.DigestNotFoundException;
import io.crate.blob.exceptions.MissingHTTPEndpointException;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.blob.v2.BlobsDisabledException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;


public class HttpBlobHandler extends SimpleChannelInboundHandler<Object> {

    private static final String SCHEME_HTTP = "http://";
    private static final String SCHEME_HTTPS = "https://";
    private static final int HTTPS_CHUNK_SIZE = 8192;
    private static final String CACHE_CONTROL_VALUE = "max-age=315360000";
    private static final String EXPIRES_VALUE = "Thu, 31 Dec 2037 23:59:59 GMT";
    private static final String BLOBS_ENDPOINT = "/_blobs";
    public static final Pattern BLOBS_PATTERN = Pattern.compile(String.format(Locale.ENGLISH, "^%s/([^_/][^/]*)/([0-9a-f]{40})$", BLOBS_ENDPOINT));
    private static final Logger LOGGER = LogManager.getLogger(HttpBlobHandler.class);

    private static final Pattern CONTENT_RANGE_PATTERN = Pattern.compile("^bytes=(\\d+)-(\\d*)$");

    private final Matcher blobsMatcher = BLOBS_PATTERN.matcher("");
    private final BlobService blobService;
    private final BlobIndicesService blobIndicesService;
    private final Netty4CorsConfig corsConfig;
    private HttpRequest currentMessage;

    private RemoteDigestBlob digestBlob;
    private ChannelHandlerContext ctx;
    private String index;
    private String digest;

    public HttpBlobHandler(BlobService blobService, BlobIndicesService blobIndicesService, Netty4CorsConfig corsConfig) {
        super(false);
        this.blobService = blobService;
        this.blobIndicesService = blobIndicesService;
        this.corsConfig = corsConfig;
    }

    private boolean possibleRedirect(HttpRequest request, String index, String digest) {
        HttpMethod method = request.method();
        if (method.equals(HttpMethod.GET) ||
            method.equals(HttpMethod.HEAD) ||
            (method.equals(HttpMethod.PUT) &&
             HttpUtil.is100ContinueExpected(request))) {
            String redirectAddress;
            try {
                redirectAddress = blobService.getRedirectAddress(index, digest);
            } catch (MissingHTTPEndpointException ex) {
                simpleResponse(request, HttpResponseStatus.BAD_GATEWAY);
                return true;
            }

            if (redirectAddress != null) {
                LOGGER.trace("redirectAddress: {}", redirectAddress);
                sendRedirect(request, activeScheme() + redirectAddress);
                return true;
            }
        }
        return false;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest request = currentMessage = (HttpRequest) msg;
            String uri = request.uri();

            if (!uri.startsWith(BLOBS_ENDPOINT)) {
                reset();
                ctx.fireChannelRead(msg);
                return;
            }

            Matcher matcher = blobsMatcher.reset(uri);
            if (!matcher.matches()) {
                simpleResponse(request, HttpResponseStatus.NOT_FOUND);
                return;
            }
            digestBlob = null;
            index = BlobIndex.fullIndexName(matcher.group(1));
            digest = matcher.group(2);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("matches index:{} digest:{}", index, digest);
                LOGGER.trace("HTTPMessage:%n{}", request);
            }
            handleBlobRequest(request, null);
        } else if (msg instanceof HttpContent) {
            if (currentMessage == null) {
                // the chunk is probably from a regular non-blob request.
                reset();
                ctx.fireChannelRead(msg);
                return;
            }

            handleBlobRequest(currentMessage, (HttpContent) msg);
        } else {
            // Neither HttpMessage or HttpChunk
            reset();
            ctx.fireChannelRead(msg);
        }
    }

    private void handleBlobRequest(HttpRequest request, @Nullable HttpContent content) throws IOException {
        if (possibleRedirect(request, index, digest)) {
            return;
        }

        HttpMethod method = request.method();
        if (method.equals(HttpMethod.GET)) {
            get(request, index, digest);
            reset();
        } else if (method.equals(HttpMethod.HEAD)) {
            head(request, index, digest);
        } else if (method.equals(HttpMethod.PUT)) {
            put(request, content, index, digest);
        } else if (method.equals(HttpMethod.DELETE)) {
            delete(request, index, digest);
        } else {
            simpleResponse(request, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    private void reset() {
        index = null;
        digest = null;
        currentMessage = null;
    }

    private void sendRedirect(HttpRequest request, String newUri) {
        HttpResponse response = prepareResponse(TEMPORARY_REDIRECT);
        response.headers().add(HttpHeaderNames.LOCATION, newUri);
        sendResponse(request, response);
    }

    private HttpResponse prepareResponse(HttpResponseStatus status) {
        HttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);
        HttpUtil.setContentLength(response, 0);
        maybeSetConnectionCloseHeader(response);
        return response;
    }

    private void simpleResponse(HttpRequest request, HttpResponseStatus status) {
        sendResponse(request, prepareResponse(status));
    }

    private void simpleResponse(HttpRequest request, HttpResponseStatus status, String body) {
        if (body == null) {
            simpleResponse(request, status);
            return;
        }
        if (!body.endsWith("\n")) {
            body += "\n";
        }
        ByteBuf content = ByteBufUtil.writeUtf8(ctx.alloc(), body);
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, content);
        HttpUtil.setContentLength(response, body.length());
        maybeSetConnectionCloseHeader(response);
        sendResponse(request, response);
    }

    private void maybeSetConnectionCloseHeader(HttpResponse response) {
        if (currentMessage == null || !HttpUtil.isKeepAlive(currentMessage)) {
            response.headers().set(HttpHeaderNames.CONNECTION, "close");
        }
    }

    private void sendResponse(HttpRequest request, HttpResponse response) {
        if (request != null) {
            Netty4CorsHandler.setCorsResponseHeaders(request, response, corsConfig);
        }
        ChannelFuture cf = ctx.channel().writeAndFlush(response);
        if (currentMessage != null && !HttpUtil.isKeepAlive(currentMessage)) {
            cf.addListener(ChannelFutureListener.CLOSE);
        }
        reset();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ClosedChannelException) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("channel closed: {}", cause.toString());
            }
            return;
        } else if (cause instanceof IOException) {
            String message = cause.getMessage();
            if (message != null && message.contains("Connection reset by peer")) {
                LOGGER.debug(message);
            } else if (cause instanceof NotSslRecordException) {
                // Raised when clients try to send unencrypted data over an encrypted channel
                // This can happen when old instances of the Admin UI are running because the
                // ports of HTTP/HTTPS are the same.
                LOGGER.debug("Received unencrypted message from '{}'", ctx.channel().remoteAddress());
            } else {
                LOGGER.warn(message, cause);
            }
            return;
        }

        HttpResponseStatus status;
        String body = null;
        if (cause instanceof DigestMismatchException || cause instanceof BlobsDisabledException
            || cause instanceof IllegalArgumentException) {
            status = HttpResponseStatus.BAD_REQUEST;
            body = String.format(Locale.ENGLISH, "Invalid request sent: %s", cause.getMessage());
        } else if (cause instanceof DigestNotFoundException || cause instanceof IndexNotFoundException) {
            status = HttpResponseStatus.NOT_FOUND;
        } else if (cause instanceof EsRejectedExecutionException) {
            status = HttpResponseStatus.TOO_MANY_REQUESTS;
            body = String.format(Locale.ENGLISH, "Rejected execution: %s", cause.getMessage());
        } else {
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            body = String.format(Locale.ENGLISH, "Unhandled exception: %s", cause);
        }
        if (body != null) {
            LOGGER.debug(body);
        }
        simpleResponse(null, status, body);
    }

    private void head(HttpRequest request, String index, String digest) throws IOException {

        // this method only supports local mode, which is ok, since there
        // should be a redirect upfront if data is not local

        BlobShard blobShard = localBlobShard(index, digest);
        long length = blobShard.blobContainer().getFile(digest).length();
        if (length < 1) {
            simpleResponse(request, HttpResponseStatus.NOT_FOUND);
            return;
        }
        HttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
        HttpUtil.setContentLength(response, length);
        setDefaultGetHeaders(response);
        sendResponse(request, response);
    }

    private void get(HttpRequest request, String index, final String digest) throws IOException {
        String range = request.headers().get(HttpHeaderNames.RANGE);
        if (range != null) {
            partialContentResponse(range, request, index, digest);
        } else {
            fullContentResponse(request, index, digest);
        }
    }

    private BlobShard localBlobShard(String index, String digest) {
        return blobIndicesService.localBlobShard(index, digest);
    }

    private void partialContentResponse(String range, HttpRequest request, String index, final String digest)
        throws IOException {
        assert range != null : "Getting partial response but no byte-range is not present.";
        Matcher matcher = CONTENT_RANGE_PATTERN.matcher(range);
        if (!matcher.matches()) {
            LOGGER.warn("Invalid byte-range: {}; returning full content", range);
            fullContentResponse(request, index, digest);
            return;
        }
        BlobShard blobShard = localBlobShard(index, digest);

        final RandomAccessFile raf = blobShard.blobContainer().getRandomAccessFile(digest);
        long start;
        long end;
        try {
            try {
                start = Long.parseLong(matcher.group(1));
                if (start > raf.length()) {
                    LOGGER.warn("416 Requested Range not satisfiable");
                    simpleResponse(request, HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
                    raf.close();
                    return;
                }
                end = raf.length() - 1;
                if (!matcher.group(2).equals("")) {
                    end = Long.parseLong(matcher.group(2));
                }
            } catch (NumberFormatException ex) {
                LOGGER.error("Couldn't parse Range Header", ex);
                start = 0;
                end = raf.length();
            }

            DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, PARTIAL_CONTENT);
            maybeSetConnectionCloseHeader(response);
            HttpUtil.setContentLength(response, end - start + 1);
            Netty4CorsHandler.setCorsResponseHeaders(request, response, corsConfig);
            response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes " + start + "-" + end + "/" + raf.length());
            setDefaultGetHeaders(response);

            ctx.channel().write(response);
            ChannelFuture writeFuture = transferFile(digest, raf, start, end - start + 1);
            if (!HttpUtil.isKeepAlive(request)) {
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable t) {
            /*
             * Make sure RandomAccessFile is closed when exception is raised.
             * In case of success, the ChannelFutureListener in "transferFile" will take care
             * that the resources are released.
             */
            raf.close();
            throw t;
        }
    }

    private void fullContentResponse(HttpRequest request, String index, final String digest) throws IOException {
        BlobShard blobShard = localBlobShard(index, digest);
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
        Netty4CorsHandler.setCorsResponseHeaders(request, response, corsConfig);
        final RandomAccessFile raf = blobShard.blobContainer().getRandomAccessFile(digest);
        try {
            maybeSetConnectionCloseHeader(response);
            HttpUtil.setContentLength(response, raf.length());
            setDefaultGetHeaders(response);
            LOGGER.trace("HttpResponse: {}", response);
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            ctx.channel().write(response);
            ChannelFuture writeFuture = transferFile(digest, raf, 0, raf.length());
            if (!keepAlive) {
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable t) {
            /*
             * Make sure RandomAccessFile is closed when exception is raised.
             * In case of success, the ChannelFutureListener in "transferFile" will take care
             * that the resources are released.
             */
            raf.close();
            throw t;
        }
    }

    private boolean sslEnabled() {
        return ctx.pipeline().get(SslHandler.class) != null;
    }

    private String activeScheme() {
        return sslEnabled() ? SCHEME_HTTPS : SCHEME_HTTP;
    }

    private ChannelFuture transferFile(final String digest,
                                       RandomAccessFile raf,
                                       long position,
                                       long count) throws IOException {
        Channel channel = ctx.channel();
        final ChannelFuture fileFuture;
        final ChannelFuture lastContentFuture;
        if (sslEnabled()) {
            var chunkedFile = new ChunkedFile(raf, 0, count, HTTPS_CHUNK_SIZE);
            fileFuture = channel.writeAndFlush(new HttpChunkedInput(chunkedFile), ctx.newProgressivePromise());

            // HttpChunkedInput also writes the end marker (LastHttpContent)
            lastContentFuture = fileFuture;
        } else {
            var region = new DefaultFileRegion(raf.getChannel(), position, count);
            fileFuture = channel.write(region, ctx.newProgressivePromise());
            lastContentFuture = channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        }

        fileFuture.addListener(new ChannelProgressiveFutureListener() {
            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) throws Exception {
                LOGGER.debug("transferFile digest={} progress={} total={}", digest, progress, total);
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) throws Exception {
                LOGGER.trace("transferFile operationComplete");
            }
        });

        return lastContentFuture;
    }

    private void setDefaultGetHeaders(HttpResponse response) {
        response.headers().set(HttpHeaderNames.ACCEPT_RANGES, "bytes");
        response.headers().set(HttpHeaderNames.EXPIRES, EXPIRES_VALUE);
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, CACHE_CONTROL_VALUE);
    }

    private void put(HttpRequest request, HttpContent content, String index, String digest) throws IOException {
        if (digestBlob == null) {
            digestBlob = blobService.newBlob(index, digest);
        }
        boolean continueExpected = HttpUtil.is100ContinueExpected(currentMessage);
        if (content == null) {
            if (continueExpected) {
                ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
            }
            return;
        }


        boolean isLast = content instanceof LastHttpContent;
        ByteBuf byteBuf = content.content();
        try {
            writeToFile(request, byteBuf, isLast, continueExpected);
        } finally {
            byteBuf.release();
        }
    }

    private void delete(HttpRequest request, String index, String digest) throws IOException {
        digestBlob = blobService.newBlob(index, digest);
        if (digestBlob.delete()) {
            // 204 for success
            simpleResponse(request, HttpResponseStatus.NO_CONTENT);
        } else {
            simpleResponse(request, HttpResponseStatus.NOT_FOUND);
        }
    }

    private void writeToFile(HttpRequest request, ByteBuf input, boolean last, final boolean continueExpected) throws IOException {
        if (digestBlob == null) {
            throw new IllegalStateException("digestBlob is null in writeToFile");
        }

        RemoteDigestBlob.Status status = digestBlob.addContent(input, last);
        HttpResponseStatus exitStatus = null;
        switch (status) {
            case FULL:
                exitStatus = HttpResponseStatus.CREATED;
                break;
            case PARTIAL:
                // tell the client to continue
                if (continueExpected) {
                    ctx.write(new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.CONTINUE));
                }
                return;
            case MISMATCH:
                exitStatus = HttpResponseStatus.BAD_REQUEST;
                break;
            case EXISTS:
                exitStatus = HttpResponseStatus.CONFLICT;
                break;
            case FAILED:
                exitStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                break;
            default:
                throw new IllegalArgumentException("Unknown status: " + status);
        }

        assert exitStatus != null : "exitStatus should not be null";
        LOGGER.trace("writeToFile exit status http:{} blob: {}", exitStatus, status);
        simpleResponse(request, exitStatus);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }
}
