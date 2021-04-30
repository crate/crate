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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

public final class Responses {

    public static FullHttpResponse contentResponse(HttpResponseStatus status, ByteBufAllocator alloc, CharSequence body) {
        var resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, ByteBufUtil.writeUtf8(alloc, body));
        HttpUtil.setContentLength(resp, body.length());
        return resp;
    }

    public static FullHttpResponse contentResponse(HttpResponseStatus status, ByteBufAllocator alloc, byte[] data) {
        ByteBuf buffer = alloc.buffer(data.length);
        buffer.writeBytes(data);
        var resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buffer);
        HttpUtil.setContentLength(resp, data.length);
        return resp;
    }

    public static FullHttpResponse redirectTo(String location) {
        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.TEMPORARY_REDIRECT);
        response.headers().add(HttpHeaderNames.LOCATION, location);
        HttpUtil.setContentLength(response, 0);
        return response;
    }
}
