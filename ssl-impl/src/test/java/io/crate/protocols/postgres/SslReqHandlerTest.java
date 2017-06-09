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

package io.crate.protocols.postgres;

import io.crate.action.sql.SQLOperations;
import io.crate.operation.auth.AuthenticationProvider;
import io.crate.protocols.postgres.ssl.SslConfigSettings;
import io.crate.protocols.postgres.ssl.SslConfigurationException;
import io.crate.protocols.postgres.ssl.SslReqConfiguringHandler;
import io.crate.protocols.postgres.ssl.SslReqHandler;
import io.crate.protocols.postgres.ssl.SslReqHandlerLoader;
import io.crate.protocols.postgres.ssl.SslReqRejectingHandler;
import io.crate.settings.SharedSettings;
import io.crate.test.integration.CrateUnitTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static io.crate.protocols.postgres.ssl.SslConfigurationTest.getAbsoluteFilePathFromClassPath;
import static io.netty.util.ReferenceCountUtil.releaseLater;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class SslReqHandlerTest extends CrateUnitTest {

    private static File trustStoreFile;
    private static File keyStoreFile;

    private EmbeddedChannel channel;

    @BeforeClass
    public static void beforeTests() throws IOException {
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.jks");
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.jks");
    }

    @After
    public void dispose() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
            channel = null;
        }
    }

    @Test
    public void testSslReqConfiguringHandler() {
        ConnectionContext ctx =
            new ConnectionContext(
                new SslReqConfiguringHandler(
                    Settings.EMPTY,
                    // use a simple ssl context
                    getSelfSignedSslContext()),
                mock(SQLOperations.class),
                AuthenticationProvider.NOOP_AUTH);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        sendSslRequest(channel);

        // We should get back an 'S'...
        ByteBuf responseBuffer = channel.readOutbound();
        byte response = responseBuffer.readByte();
        assertEquals(response, 'S');

        // ...and continue encrypted (ssl handler)
        assertThat(channel.pipeline().first(), instanceOf(SslHandler.class));
    }

    @Test
    public void testClassLoadingFallback() {
        {
            Settings settings = Settings.builder()
                .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), false)
                .put(SslConfigSettings.SSL_ENABLED.getKey(), true)
                .build();
            assertThat(SslReqHandlerLoader.load(settings), instanceOf(SslReqRejectingHandler.class));
        }
        {
            Settings settings = Settings.builder()
                .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), true)
                .put(SslConfigSettings.SSL_ENABLED.getKey(), false)
                .build();
            assertThat(SslReqHandlerLoader.load(settings), instanceOf(SslReqRejectingHandler.class));
        }
        {
            Settings settings = Settings.builder()
                .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), false)
                .put(SslConfigSettings.SSL_ENABLED.getKey(), false)
                .build();
            assertThat(SslReqHandlerLoader.load(settings), instanceOf(SslReqRejectingHandler.class));
        }
    }

    @Test
    public void testClassLoadingWithInvalidConfiguration() {
        expectedException.expect(SslConfigurationException.class);
        expectedException.expectMessage("Failed to build SSL configuration");
        // empty ssl configuration which is invalid
        Settings enterpriseEnabled = Settings.builder()
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), true)
            .put(SslConfigSettings.SSL_ENABLED.getKey(), true)
            .build();
        SslReqHandlerLoader.load(enterpriseEnabled);
    }

    @Test
    public void testClassLoadingWithValidConfiguration() {
        Settings enterpriseEnabled = Settings.builder()
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), true)
            .put(SslConfigSettings.SSL_ENABLED.getKey(), true)
            .put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile)
            .put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "truststorePassword")
            .put(SslConfigSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile)
            .put(SslConfigSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "serverKeyPassword")
            .build();
        assertThat(SslReqHandlerLoader.load(enterpriseEnabled), instanceOf(SslReqConfiguringHandler.class));
    }

    private static void sendSslRequest(EmbeddedChannel channel) {
        ByteBuf buffer = releaseLater(Unpooled.buffer());
        buffer.writeInt(SslReqHandler.SSL_REQUEST_BYTE_LENGTH);
        buffer.writeInt(SslReqHandler.SSL_REQUEST_CODE);
        channel.writeInbound(buffer);
    }

    /**
     * Uses a simple (and insecure) self-signed certificate.
     */
    private static SslContext getSelfSignedSslContext () {
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
}
