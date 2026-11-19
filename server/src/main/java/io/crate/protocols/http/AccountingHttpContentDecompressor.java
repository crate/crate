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

import static io.netty.handler.codec.http.HttpHeaderValues.DEFLATE;
import static io.netty.handler.codec.http.HttpHeaderValues.GZIP;
import static io.netty.handler.codec.http.HttpHeaderValues.X_DEFLATE;
import static io.netty.handler.codec.http.HttpHeaderValues.X_GZIP;

import java.util.function.Supplier;

import org.elasticsearch.common.breaker.CircuitBreaker;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpContentDecompressor;

/// Accounts the decompressed content of gzip and deflate encoded requests,
/// until the content is released, see {@link AccountingZlibDecoder}.
///
/// {@code http.max_content_length} limits each request individually, but highly compressed content
/// of many concurrent requests could still exhaust the heap while being aggregated.
/// Other encodings are decoded by {@link HttpContentDecompressor} without accounting.
public class AccountingHttpContentDecompressor extends HttpContentDecompressor {

    private final Supplier<CircuitBreaker> circuitBreaker;

    public AccountingHttpContentDecompressor(Supplier<CircuitBreaker> circuitBreaker) {
        super(0);
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) throws Exception {
        if (GZIP.contentEqualsIgnoreCase(contentEncoding) || X_GZIP.contentEqualsIgnoreCase(contentEncoding)) {
            return zlibDecoder(ZlibWrapper.GZIP);
        }
        if (DEFLATE.contentEqualsIgnoreCase(contentEncoding) || X_DEFLATE.contentEqualsIgnoreCase(contentEncoding)) {
            return zlibDecoder(ZlibWrapper.ZLIB_OR_NONE);
        }
        return super.newContentDecoder(contentEncoding);
    }

    private EmbeddedChannel zlibDecoder(ZlibWrapper wrapper) {
        Channel channel = ctx.channel();
        return EmbeddedChannel.builder()
            .channelId(channel.id())
            .hasDisconnect(channel.metadata().hasDisconnect())
            .config(channel.config())
            .handlers(new AccountingZlibDecoder(wrapper, circuitBreaker))
            .build();
    }
}
