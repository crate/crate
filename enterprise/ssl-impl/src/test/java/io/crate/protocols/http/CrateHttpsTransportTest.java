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

import static io.crate.protocols.ssl.SslConfigurationTest.getAbsoluteFilePathFromClassPath;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.Security;
import java.util.Collections;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.netty.EventLoopGroups;
import io.crate.plugin.PipelineRegistry;
import io.crate.protocols.ssl.SslConfigSettings;
import io.crate.protocols.ssl.SslContextProviderImpl;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;

public class CrateHttpsTransportTest extends ESTestCase {

    private static File trustStoreFile;
    private static File keyStoreFile;
    private static String defaultKeyStoreType = KeyStore.getDefaultType();

    @BeforeClass
    public static void beforeTests() throws IOException {
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.jks");
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.jks");
        Security.setProperty("keystore.type", "jks");
    }

    @AfterClass
    public static void resetKeyStoreType() {
        Security.setProperty("keystore.type", defaultKeyStoreType);
    }

    @Test
    public void testPipelineConfiguration() throws Exception {
        Settings settings = Settings.builder()
            .put(PATH_HOME_SETTING.getKey(), "/tmp")
            .put(SslConfigSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "truststorePassword")
            .put(SslConfigSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "serverKeyPassword")
            .build();

        NetworkService networkService = new NetworkService(Collections.singletonList(new NetworkService.CustomNameResolver() {
            @Override
            public InetAddress[] resolveDefault() {
                return new InetAddress[] { InetAddresses.forString("127.0.0.1") };
            }

            @Override
            public InetAddress[] resolveIfPossible(String value) throws IOException {
                return new InetAddress[] { InetAddresses.forString("127.0.0.1") };
            }
        }));

        PipelineRegistry pipelineRegistry = new PipelineRegistry(settings);
        pipelineRegistry.setSslContextProvider(new SslContextProviderImpl(settings));

        Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
            settings,
            networkService,
            BigArrays.NON_RECYCLING_INSTANCE,
            mock(ThreadPool.class),
            NamedXContentRegistry.EMPTY,
            pipelineRegistry,
            new EventLoopGroups(),
            mock(NodeClient.class));

        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            transport.start();

            Netty4HttpServerTransport.HttpChannelHandler httpChannelHandler =
                (Netty4HttpServerTransport.HttpChannelHandler) transport.configureServerChannelHandler();

            httpChannelHandler.initChannel(channel);

            assertThat(channel.pipeline().first(), instanceOf(SslHandler.class));

        } finally {
            transport.stop();
            transport.close();
            channel.releaseInbound();
            channel.close().awaitUninterruptibly();
        }
    }
}
