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

package io.crate.protocols.postgres;

import io.crate.action.sql.SQLOperations;
import io.crate.auth.AccessControl;
import io.crate.auth.AlwaysOKAuthentication;
import io.crate.auth.Protocol;
import io.crate.protocols.ssl.SslContextProvider;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class SslReqHandlerTest extends ESTestCase {

    private EmbeddedChannel channel;

    @BeforeClass
    public static void ensureEnglishLocale() {
        // BouncyCastle is parsing date objects with the system locale while creating self-signed SSL certs
        // This fails for certain locales, e.g. 'ks'.
        // Until this is fixed, we force the english locale.
        // See also https://github.com/bcgit/bc-java/issues/405 (different topic, but same root cause)
        Locale.setDefault(Locale.ENGLISH);
    }

    @After
    public void dispose() {
        if (channel != null) {
            channel.releaseInbound();
            channel.close().awaitUninterruptibly();
            channel = null;
        }
    }

    @Test
    public void testSslReqHandler() {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                mock(SQLOperations.class),
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> null),
                // use a simple ssl context
                null, getSelfSignedSslContextProvider());

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        sendSslRequest(channel);

        // We should get back an 'S'...
        ByteBuf responseBuffer = channel.readOutbound();
        byte response = responseBuffer.readByte();
        assertEquals(response, 'S');
        responseBuffer.release();

        // ...and continue encrypted (ssl handler)
        assertThat(channel.pipeline().first(), instanceOf(SslHandler.class));
    }

    private static void sendSslRequest(EmbeddedChannel channel) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(SslReqHandler.SSL_REQUEST_BYTE_LENGTH);
        buffer.writeInt(SslReqHandler.SSL_REQUEST_CODE);
        channel.writeInbound(buffer);
    }

    /**
     * Uses a simple (and insecure) self-signed certificate.
     */
    private static SslContextProvider getSelfSignedSslContextProvider() {
        return new SslContextProvider(Settings.EMPTY) {

            @Override
            public SslContext getServerContext(Protocol protocol) {
                try {
                    SelfSignedCertificate ssc = new SelfSignedCertificate();
                    return SslContextBuilder
                        .forServer(ssc.certificate(), ssc.privateKey())
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .startTls(false)
                        .build();
                } catch (Exception e) {
                    throw new RuntimeException("Couldn't setup self signed certificate", e);
                }
            }

        };
    }
}
