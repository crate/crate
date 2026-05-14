/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http.netty4;

import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;

/**
 * Adds an {@code Alt-Svc} response header on the TCP/HTTPS path so clients can discover HTTP/3 (QUIC).
 */
public final class Netty4AltSvcHandler extends ChannelDuplexHandler {

    private final Supplier<@Nullable String> altSvcHeaderSupplier;

    Netty4AltSvcHandler(Supplier<@Nullable String> altSvcHeaderSupplier) {
        this.altSvcHeaderSupplier = altSvcHeaderSupplier;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof HttpResponse response) {
            String altSvc = altSvcHeaderSupplier.get();
            if (altSvc != null) {
                response.headers().set(HttpHeaderNames.ALT_SVC, altSvc);
            }
        }
        ctx.write(msg, promise);
    }
}
