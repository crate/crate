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

package io.crate.http.netty;

import io.crate.blob.BlobService;
import io.crate.blob.DigestBlob;
import io.crate.blob.RemoteDigestBlob;
import io.crate.blob.exceptions.DigestMismatchException;
import io.crate.blob.exceptions.DigestNotFoundException;
import io.crate.blob.exceptions.MissingHTTPEndpointException;
import io.crate.blob.v2.BlobIndices;
import io.crate.blob.v2.BlobShard;
import io.crate.blob.v2.BlobsDisabledException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.CharsetUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.jboss.netty.channel.Channels.succeededFuture;
import static org.jboss.netty.channel.Channels.write;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpBlobHandler extends SimpleChannelUpstreamHandler implements
        LifeCycleAwareChannelHandler {

    private static final String CACHE_CONTROL_VALUE = "max-age=315360000";
    private static final String EXPIRES_VALUE = "Thu, 31 Dec 2037 23:59:59 GMT";
    private static final String BLOBS_ENDPOINT = "/_blobs";
    public static final Pattern BLOBS_PATTERN = Pattern.compile(String.format("^%s/([^_/][^/]*)/([0-9a-f]{40})$", BLOBS_ENDPOINT));
    private static final ESLogger LOGGER = Loggers.getLogger(HttpBlobHandler.class);

    private static final ChannelBuffer CONTINUE = ChannelBuffers.copiedBuffer(
            "HTTP/1.1 100 Continue\r\n\r\n", CharsetUtil.US_ASCII);

    private static final Pattern CONTENT_RANGE_PATTERN = Pattern.compile("^bytes=(\\d+)-(\\d*)$");

    private final Matcher blobsMatcher = BLOBS_PATTERN.matcher("");
    private final BlobService blobService;
    private final BlobIndices blobIndices;
    private HttpMessage currentMessage;
    private ChannelHandlerContext ctx;

    private RemoteDigestBlob digestBlob;

    public HttpBlobHandler(BlobService blobService, BlobIndices blobIndices) {
        this.blobService = blobService;
        this.blobIndices = blobIndices;
    }


    private boolean possibleRedirect(HttpRequest request, String index, String digest) {
        HttpMethod method = request.getMethod();
        if (method.equals(HttpMethod.GET) ||
                method.equals(HttpMethod.HEAD) ||
                (method.equals(HttpMethod.PUT) &&
                        HttpHeaders.is100ContinueExpected(request))) {
            String redirectAddress = null;
            try {
                redirectAddress = blobService.getRedirectAddress(index, digest);
            } catch (MissingHTTPEndpointException ex) {
                simpleResponse(HttpResponseStatus.BAD_GATEWAY, null);
                return true;
            }
            if (redirectAddress != null) {
                LOGGER.trace("redirectAddress: {}", redirectAddress);
                sendRedirect(request, redirectAddress);
                return true;
            }
        }
        return false;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();

        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;
            String uri = request.getUri();

            if (!uri.startsWith(BLOBS_ENDPOINT)) {
                reset();
                ctx.sendUpstream(e);
                return;
            }

            Matcher matcher = blobsMatcher.reset(uri);
            if (!matcher.matches()) {
                reset();
                simpleResponse(HttpResponseStatus.NOT_FOUND, null);
                return;
            }

            handleBlobRequest(request, matcher);
        } else if (msg instanceof HttpChunk) {
            if (currentMessage == null) {
                // the chunk is probably from a regular non-blob request.
                ctx.sendUpstream(e);
                return;
            }
            HttpChunk chunk = (HttpChunk) msg;
            writeToFile(chunk.getContent(), chunk.isLast(), false);
            if (chunk.isLast()) {
                reset();
            }
        } else {
            // Neither HttpMessage or HttpChunk
            ctx.sendUpstream(e);
        }
    }

    private void handleBlobRequest(HttpRequest request, Matcher matcher) throws IOException {
        digestBlob = null;
        String index = matcher.group(1);
        String digest = matcher.group(2);

        LOGGER.trace("matches index:{} digest:{}", index, digest);
        LOGGER.trace("HTTPMessage:%n{}", request);

        index = BlobIndices.fullIndexName(index);

        if (possibleRedirect(request, index, digest)) {
            reset();
            return;
        }

        if (request.getMethod().equals(HttpMethod.GET)) {
            get(request, index, digest);
            reset();
        } else if (request.getMethod().equals(HttpMethod.HEAD)) {
            head(request, index, digest);
            reset();
        } else if (request.getMethod().equals(HttpMethod.PUT)) {
            put(request, index, digest);
        } else if (request.getMethod().equals(HttpMethod.DELETE)) {
            delete(index, digest);
            reset();
        } else {
            simpleResponse(HttpResponseStatus.METHOD_NOT_ALLOWED, null);
            reset();
        }
    }

    private void reset() {
        currentMessage = null;
    }

    private void sendRedirect(HttpRequest request, String newUri) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.TEMPORARY_REDIRECT);
        HttpHeaders.setContentLength(response, 0);
        response.headers().add(HttpHeaders.Names.LOCATION, newUri);
        ChannelFuture cf = ctx.getChannel().write(response);
        if (!HttpHeaders.isKeepAlive(request)) {
            cf.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void simpleResponse(HttpResponseStatus status, String body) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                status);

        if (body != null && body.length() > 0) {
            if (!body.endsWith("\n")) {
                body += "\n";
            }
            HttpHeaders.setContentLength(response, body.length());
            response.setContent(ChannelBuffers.copiedBuffer(body, CharsetUtil.UTF_8));
        } else {
            HttpHeaders.setContentLength(response, 0);
        }
        reset();
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        Throwable ex = e.getCause();
        if (ex instanceof ClosedChannelException) {
            LOGGER.trace("channel closed: {}", ex.toString());
            return;
        } else if (ex instanceof IOException) {
            String message = ex.getMessage();
            if (message != null && message.contains("Connection reset by peer")) {
                LOGGER.debug(message);
            } else {
                LOGGER.warn(message, e);
            }
            return;
        }

        HttpResponseStatus status;
        String body = ex.toString();
        if (ex instanceof DigestMismatchException) {
            status = HttpResponseStatus.BAD_REQUEST;
        } else if (ex instanceof DigestNotFoundException) {
            status = HttpResponseStatus.NOT_FOUND;
            body = null;
        } else if (ex instanceof BlobsDisabledException || ex instanceof IndexNotFoundException) {
            status = HttpResponseStatus.BAD_REQUEST;
            body = ex.getMessage();
        } else if (ex instanceof EsRejectedExecutionException) {
            status = TOO_MANY_REQUESTS;
            body = ex.getMessage();
        } else {
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            LOGGER.error("unhandled exception:", ex);
        }
        simpleResponse(status, body);
    }

    private void head(HttpRequest request, String index, String digest) throws IOException {

        // this method only supports local mode, which is ok, since there
        // should be a redirect upfront if data is not local

        BlobShard blobShard = localBlobShard(index, digest);
        long length = blobShard.blobContainer().getFile(digest).length();
        if (length < 1) {
            simpleResponse(HttpResponseStatus.NOT_FOUND, null);
            return;
        }
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpHeaders.setContentLength(response, length);
        setDefaultGetHeaders(response);
        ChannelFuture cf = ctx.getChannel().write(response);
        if (!HttpHeaders.isKeepAlive(request)) {
            cf.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void get(HttpRequest request, String index, final String digest) throws IOException {
        String range = request.headers().get(RANGE);
        if (range != null) {
            partialContentResponse(range, request, index, digest);
        } else {
            fullContentResponse(request, index, digest);
        }
    }

    private BlobShard localBlobShard(String index, String digest){
        return blobIndices.localBlobShard(index, digest);
    }

    private void partialContentResponse(String range, HttpRequest request, String index, final String digest)
        throws  IOException {
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
                    simpleResponse(HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE, null);
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

            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, PARTIAL_CONTENT);
            HttpHeaders.setContentLength(response, end - start + 1);
            response.headers().set(CONTENT_RANGE, "bytes " + start + "-" + end + "/" + raf.length());
            setDefaultGetHeaders(response);

            ctx.getChannel().write(response);
            ChannelFuture writeFuture = transferFile(digest, raf, start, end - start + 1);
            if (!HttpHeaders.isKeepAlive(request)) {
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable t) {
            /**
             * Make sure RandomAccessFile is closed when exception is raised.
             * In case of success, the ChannelFutureListener in "transferFile" will take care
             * that the resources are released.
             */
            raf.close();
            throw t;
        }
    }

    private void fullContentResponse(HttpRequest request, String index, final String digest) throws  IOException {
        BlobShard blobShard = localBlobShard(index, digest);
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        final RandomAccessFile raf = blobShard.blobContainer().getRandomAccessFile(digest);
        try {
            HttpHeaders.setContentLength(response, raf.length());
            setDefaultGetHeaders(response);
            LOGGER.trace("HttpResponse: {}", response);
            ctx.getChannel().write(response);
            ChannelFuture writeFuture = transferFile(digest, raf, 0, raf.length());
            if (!HttpHeaders.isKeepAlive(request)) {
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable t) {
            /**
             * Make sure RandomAccessFile is closed when exception is raised.
             * In case of success, the ChannelFutureListener in "transferFile" will take care
             * that the resources are released.
             */
            raf.close();
            throw t;
        }
    }

    private ChannelFuture transferFile(final String digest, RandomAccessFile raf, long position, long count)
        throws IOException
    {
        final FileRegion region = new DefaultFileRegion(raf.getChannel(), position, count);
        ChannelFuture writeFuture = ctx.getChannel().write(region);
        writeFuture.addListener(new ChannelFutureProgressListener() {
            @Override
            public void operationProgressed(ChannelFuture future, long amount, long current, long total) throws Exception {
                LOGGER.debug("{}: {} / {} (+{})", digest, current, total, amount);
            }

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                region.releaseExternalResources();
                LOGGER.trace("file transfer completed");
            }
        });
        return writeFuture;
    }

    private void setDefaultGetHeaders(HttpResponse response) {
        response.headers().set(ACCEPT_RANGES, "bytes");
        response.headers().set(EXPIRES, EXPIRES_VALUE);
        response.headers().set(CACHE_CONTROL, CACHE_CONTROL_VALUE);
    }

    private void put(HttpRequest request, String index, String digest) throws IOException {

        if (digestBlob != null) {
            throw new IllegalStateException(
                    "received new PUT Request " + HttpRequest.class.getSimpleName() +
                            "with existing " + DigestBlob.class.getSimpleName());
        }

        // shortcut check if the file existsLocally locally, so we can immediatly return
        // if (blobService.existsLocally(digest)) {
        //    simpleResponse(HttpResponseStatus.CONFLICT, null);
        //}

        // TODO: Respond with 413 Request Entity Too Large

        digestBlob = blobService.newBlob(index, digest);
        currentMessage = request;

        if (request.isChunked()) {
            writeToFile(request.getContent(), false, HttpHeaders.is100ContinueExpected(request));
        } else {
            writeToFile(request.getContent(), true, HttpHeaders.is100ContinueExpected(request));
            reset();
        }
    }

    private void delete(String index, String digest) throws IOException {
        digestBlob = blobService.newBlob(index, digest);
        if (digestBlob.delete()) {
             // 204 for success
            simpleResponse(HttpResponseStatus.NO_CONTENT, null);
        } else {
            simpleResponse(HttpResponseStatus.NOT_FOUND, null);
        }
    }

    protected void writeToFile(ChannelBuffer input, boolean last, final boolean continueExpected) throws
            IOException {
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
                    write(ctx, succeededFuture(ctx.getChannel()), CONTINUE.duplicate());
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
        }

        assert exitStatus != null;
        LOGGER.trace("writeToFile exit status http:{} blob: {}", exitStatus, status);
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, exitStatus);
        HttpHeaders.setContentLength(response, 0);
        ChannelFuture cf = ctx.getChannel().write(response);
        if (currentMessage == null || !HttpHeaders.isKeepAlive(currentMessage)) {
            cf.addListener(ChannelFutureListener.CLOSE);
        }
    }

    public void beforeAdd(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        // noop
    }

    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
        // noop
    }

    public void afterRemove(ChannelHandlerContext ctx) throws Exception {
        // noop
    }
}