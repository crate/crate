/*
///Licensed to Crate.io GmbH ("Crate") under one or more contributor
///license agreements.  See the NOTICE file distributed with this work for
///additional information regarding copyright ownership.  Crate licenses
///this file to you under the Apache License, Version 2.0 (the "License");
///you may not use this file except in compliance with the License.  You may
///obtain a copy of the License at
 *
///  http://www.apache.org/licenses/LICENSE-2.0
 *
///Unless required by applicable law or agreed to in writing, software
///distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
///WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
///License for the specific language governing permissions and limitations
///under the License.
 *
///However, if you have executed another commercial license agreement
///with Crate these terms will supersede the license and you may use the
///software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.protocols.http;

import java.util.ArrayDeque;
import java.util.function.Supplier;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jspecify.annotations.Nullable;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

/// Accounts the (decompressed) content of HTTP with the
/// {@link CircuitBreaker#IN_FLIGHT_REQUESTS} circuit breaker.
///
/// {@code http.max_content_length} limits each request individually, but many concurrent
/// requests could still exhaust the heap while their content is being aggregated.
///
/// - The reserved memory is released once its response is written,
///   or when the channel is closed.
/// - Content of blob requests is not accounted as it is streamed to disk.
public class InFlightRequestsBreakerHandler extends ChannelDuplexHandler {

    static final String LABEL = "http_request";

    private final Supplier<CircuitBreaker> breaker;

    private final ArrayDeque<Reservation> pending = new ArrayDeque<>();

    @Nullable
    private Reservation current;

    private boolean discarding = false;
    private boolean infoResponse = false;

    private static final class Reservation {
        private final boolean accounted;
        private long bytes = 0;
        private boolean released = false;

        private Reservation(boolean accounted) {
            this.accounted = accounted;
        }
    }

    public InFlightRequestsBreakerHandler(Supplier<CircuitBreaker> breaker) {
        this.breaker = breaker;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest request) {
            discarding = false;
            current = new Reservation(HttpBlobHandler.isNotBlobRequest(request.uri()));
            pending.add(current);
        } else if (discarding) {
            if (msg instanceof LastHttpContent) {
                discarding = false;
            }
            ReferenceCountUtil.release(msg);
            return;
        }
        if (msg instanceof HttpContent content && current != null && current.accounted && !current.released) {
            int bytes = content.content().readableBytes();
            if (bytes > 0) {
                try {
                    breaker.get().addEstimateBytesAndMaybeBreak(bytes, LABEL);
                } catch (CircuitBreakingException e) {
                    ReferenceCountUtil.release(msg);
                    discarding = !(msg instanceof LastHttpContent);
                    reject(ctx, e);
                    return;
                }
                current.bytes += bytes;
            }
        }
        if (msg instanceof LastHttpContent) {
            current = null;
        }
        ctx.fireChannelRead(msg);
    }

    private void reject(ChannelHandlerContext ctx, CircuitBreakingException e) {
        Reservation rejected = current;
        current = null;
        if (rejected != null) {
            pending.remove(rejected);
            release(rejected);
        }
        FullHttpResponse response = Responses.contentResponse(
            HttpResponseStatus.TOO_MANY_REQUESTS,
            ctx.alloc(),
            e.getMessage() + "\n"
        );
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        // Close the channel to make the aggregator release partial content.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof HttpResponse response) {
            infoResponse = response.status().codeClass() == HttpStatusClass.INFORMATIONAL;
        }
        if ((msg instanceof LastHttpContent || msg instanceof HttpChunkedInput) && !infoResponse) {
            Reservation reservation = pending.poll();
            if (reservation != null) {
                release(reservation);
            }
        }
        ctx.write(msg, promise);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseAll();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        releaseAll();
        super.handlerRemoved(ctx);
    }

    private void releaseAll() {
        Reservation reservation;
        while ((reservation = pending.poll()) != null) {
            release(reservation);
        }
        current = null;
    }

    private void release(Reservation reservation) {
        if (reservation.released) {
            return;
        }
        reservation.released = true;
        if (reservation.bytes > 0) {
            breaker.get().addWithoutBreaking(-reservation.bytes);
            reservation.bytes = 0;
        }
    }
}
