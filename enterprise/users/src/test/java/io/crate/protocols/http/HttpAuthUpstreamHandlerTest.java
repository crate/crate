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

import io.crate.operation.auth.AlwaysOKAuthentication;
import io.crate.operation.auth.Authentication;
import io.crate.operation.auth.HostBasedAuthentication;
import io.crate.test.integration.CrateUnitTest;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import javax.net.ssl.SSLSession;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpAuthUpstreamHandlerTest extends CrateUnitTest {

    private final Settings settings = Settings.builder()
        .put("auth.host_based.enabled", true)
        .put("auth.host_based.config.0.user", "crate")
        .build();

    // UserLookup always returns null, so there are no users (even no default crate super user)
    private final Authentication authService = new HostBasedAuthentication(settings, userName -> null);

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
        HttpAuthUpstreamHandler handler = new HttpAuthUpstreamHandler(Settings.EMPTY, new AlwaysOKAuthentication());
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
        request.headers().add("X-User", "Arthur");
        request.headers().add("X-Real-Ip", "10.1.0.100");

        ch.writeInbound(request);
        assertFalse(handler.authorized());

        assertUnauthorized(
            ch.readOutbound(),
            "No valid auth.host_based.config entry found for host \"10.1.0.100\", user \"Arthur\", protocol \"http\"\n");
    }

    @Test
    public void testUnauthorizedUser() throws Exception {
        HttpAuthUpstreamHandler handler = new HttpAuthUpstreamHandler(Settings.EMPTY, authService);
        EmbeddedChannel ch = new EmbeddedChannel(handler);

        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/_sql");

        ch.writeInbound(request);

        assertFalse(handler.authorized());
        assertUnauthorized(ch.readOutbound(), "trust authentication failed for user \"crate\"\n");
    }

    @Test
    public void testClientCertUserHasPreferenceOverTrustAuthDefault() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        SSLSession session = mock(SSLSession.class);
        when(session.getPeerCertificates()).thenReturn(new Certificate[] { ssc.cert() });

        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/_sql");
        String userName = HttpAuthUpstreamHandler.userFromRequest(request, session, Settings.EMPTY);

        assertThat(userName, is("example.com"));
    }
}
