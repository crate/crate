/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.protocols.http;

import io.crate.operation.auth.Authentication;
import io.crate.operation.auth.AuthenticationMethod;
import io.crate.operation.auth.AuthenticationProvider;
import io.crate.operation.auth.HbaProtocol;
import io.crate.operation.user.User;
import io.crate.test.integration.CrateUnitTest;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.core.Is.is;

public class HttpAuthUpstreamHandlerTest extends CrateUnitTest {

    private final AuthenticationMethod denyAll = new AuthenticationMethod() {

        @Override
        public User authenticate(String userName) {
            throw new RuntimeException("denied");
        }

        @Override
        public String name() {
            return "denyAll";
        }
    };

    private final Authentication authService = new Authentication() {
        @Override
        public boolean enabled() {
            return true;
        }

        @Nullable
        @Override
        public AuthenticationMethod resolveAuthenticationType(String user, InetAddress address, HbaProtocol protocol) {
            // we want to test two things:
            // 1) the user "null" does not have a hba entry,
            //    therefore this method does not return an authentication method
            // 2) all other users have an entry, but they are always denied
            if ("null".equals(user)) {
                return null;
            }
            return denyAll;
        }
    };

    private static void assertUnauthorized(DefaultFullHttpResponse resp, String expectedBody) {
        assertThat(resp.status(), is(HttpResponseStatus.UNAUTHORIZED));
        assertThat(resp.content().toString(StandardCharsets.UTF_8), is(expectedBody));
    }

    @Test
    public void testChannelClosedWhenUnauthorized() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpAuthUpstreamHandler.sendUnauthorized(ch, null);

        HttpResponse resp = ch.readOutbound();
        assertThat(resp.status(), is(HttpResponseStatus.UNAUTHORIZED));
        assertThat(ch.isOpen(), is(false));
    }

    @Test
    public void testSendUnauthorizedWithoutBody() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpAuthUpstreamHandler.sendUnauthorized(ch, null);

        DefaultFullHttpResponse resp = ch.readOutbound();
        assertThat(resp.content(), is(Unpooled.EMPTY_BUFFER));
    }

    @Test
    public void testSendUnauthorizedWithBody() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpAuthUpstreamHandler.sendUnauthorized(ch, "not allowed\n");

        DefaultFullHttpResponse resp = ch.readOutbound();
        assertThat(resp.content().toString(StandardCharsets.UTF_8), is("not allowed\n"));
    }

    @Test
    public void testSendUnauthorizedWithBodyNoNewline() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpAuthUpstreamHandler.sendUnauthorized(ch, "not allowed");

        DefaultFullHttpResponse resp = ch.readOutbound();
        assertThat(resp.content().toString(StandardCharsets.UTF_8), is("not allowed\n"));
    }

    @Test
    public void testAuthorized() throws Exception {
        HttpAuthUpstreamHandler handler = new HttpAuthUpstreamHandler(Settings.EMPTY, AuthenticationProvider.NOOP_AUTH);
        EmbeddedChannel ch = new EmbeddedChannel(handler);

        DefaultHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/_sql");
        ch.writeInbound(request);

        assertThat(handler.authorized(), is(true));
    }


    @Test
    public void testNotNoHbaConfig() throws Exception {
        HttpAuthUpstreamHandler handler = new HttpAuthUpstreamHandler(Settings.EMPTY, authService);
        EmbeddedChannel ch = new EmbeddedChannel(handler);

        DefaultHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/_sql");
        request.headers().add("X-User", "null");
        request.headers().add("X-Real-Ip", "10.1.0.100");

        ch.writeInbound(request);
        assertFalse(handler.authorized());

        assertUnauthorized(
            ch.readOutbound(),
            "No valid auth.host_based.config entry found for host \"10.1.0.100\", user \"null\", protocol \"http\"\n");
    }

    @Test
    public void testUnauthorizedUser() throws Exception {
        HttpAuthUpstreamHandler handler = new HttpAuthUpstreamHandler(Settings.EMPTY, authService);
        EmbeddedChannel ch = new EmbeddedChannel(handler);

        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/_sql");

        ch.writeInbound(request);

        assertFalse(handler.authorized());
        assertUnauthorized(ch.readOutbound(), "denied\n");
    }
}
