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

import org.elasticsearch.http.netty.pipelining.OrderedUpstreamMessageEvent;
import org.elasticsearch.rest.support.RestUtils;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.regex.Pattern;


/**
 *
 */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

    private final NettyHttpServerTransport serverTransport;
    private final Pattern corsPattern;
    private final boolean httpPipeliningEnabled;
    private final boolean detailedErrorsEnabled;

    public HttpRequestHandler(NettyHttpServerTransport serverTransport, boolean detailedErrorsEnabled) {
        this.serverTransport = serverTransport;
        this.corsPattern = RestUtils.checkCorsSettingForRegex(NettyHttpServerTransport.SETTING_CORS_ALLOW_ORIGIN);
        this.httpPipeliningEnabled = serverTransport.settings().getAsBoolean(NettyHttpServerTransport.SETTING_PIPELINING, NettyHttpServerTransport.DEFAULT_SETTING_PIPELINING);
        this.detailedErrorsEnabled = detailedErrorsEnabled;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        HttpRequest request;
        OrderedUpstreamMessageEvent oue = null;
        if (this.httpPipeliningEnabled && e instanceof OrderedUpstreamMessageEvent) {
            oue = (OrderedUpstreamMessageEvent) e;
            request = (HttpRequest) oue.getMessage();
        } else {
            request = (HttpRequest) e.getMessage();
        }

        // the netty HTTP handling always copy over the buffer to its own buffer, either in NioWorker internally
        // when reading, or using a cumalation buffer
        NettyHttpRequest httpRequest = new NettyHttpRequest(request, e.getChannel());
        if (oue != null) {
            serverTransport.dispatchRequest(httpRequest, new NettyHttpChannel(serverTransport, httpRequest, corsPattern, oue, detailedErrorsEnabled));
        } else {
            serverTransport.dispatchRequest(httpRequest, new NettyHttpChannel(serverTransport, httpRequest, corsPattern, detailedErrorsEnabled));
        }
        super.messageReceived(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        serverTransport.exceptionCaught(ctx, e);
    }
}
