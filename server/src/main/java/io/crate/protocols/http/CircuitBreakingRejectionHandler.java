/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jspecify.annotations.Nullable;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

/// Rejects a request with {@code 429 Too Many Requests} if a {@link CircuitBreakingException} is raised
/// while it's decoded, e.g. by {@link AccountingHttpContentDecompressor}.
///
/// The connection is closed, as the decoder state is unusable, and the aggregator releases
/// any partial content.
public class CircuitBreakingRejectionHandler extends ChannelInboundHandlerAdapter {

    private boolean rejected = false;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (rejected) {
            ReferenceCountUtil.release(msg);
            return;
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (rejected) {
            // the output dropped by the trip is missing (e.g. a CRC mismatch). The channel is closing anyway.
            return;
        }
        CircuitBreakingException e = extractCircuitBreakingException(cause);
        if (e == null) {
            ctx.fireExceptionCaught(cause);
            return;
        }
        rejected = true;
        FullHttpResponse response = Responses.contentResponse(
            HttpResponseStatus.TOO_MANY_REQUESTS,
            ctx.alloc(),
            e.getMessage() + "\n"
        );
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Nullable
    private static CircuitBreakingException extractCircuitBreakingException(Throwable t) {
        while (t != null) {
            if (t instanceof CircuitBreakingException e) {
                return e;
            }
            if (t.getCause() == t) {
                return null;
            }
            t = t.getCause();
        }
        return null;
    }
}
