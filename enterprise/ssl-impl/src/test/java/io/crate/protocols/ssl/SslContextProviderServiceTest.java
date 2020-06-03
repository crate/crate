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

package io.crate.protocols.ssl;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.netty.handler.ssl.SslContext;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;

public class SslContextProviderServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_service_reloads_configuration_on_change_of_symlinked_files() throws Exception {
        /*
         * Emulate kubernetes like folder structure (CertManager/mounted secrets)
         * See https://github.com/crate/crate/issues/10022 for context
         *
         * root@test-7b9c78557b-2xh5w:/# ls -las /usr/local/share/credentials/
         * total 0
         * 0 drwxrwxrwt 3 root root  160 May 29 07:54 .
         * 0 drwxrwsr-x 1 root staff  85 May 29 07:07 ..
         * 0 drwxr-xr-x 2 root root  120 May 29 07:54 ..2020_05_29_07_54_27.931224672
         * 0 lrwxrwxrwx 1 root root   31 May 29 07:54 ..data -> ..2020_05_29_07_54_27.931224672
         * 0 lrwxrwxrwx 1 root root   19 May 29 07:53 keystore.jks -> ..data/keystore.jks
         */

        Path tempDir = createTempDir();
        Path dataTarget05 = Files.createDirectory(tempDir.resolve("2020_05"));
        Path dataTarget06 = Files.createDirectory(tempDir.resolve("2020_06"));
        Path dataLink = tempDir.resolve("data");

        Files.createSymbolicLink(dataLink, dataTarget05);
        Files.write(dataLink.resolve("keystore.jks"), List.of("version1"));

        Path keystoreLink = tempDir.resolve("keystore.jks");
        Path keystoreTarget = tempDir.resolve("data/keystore.jks");
        Files.createSymbolicLink(keystoreLink, keystoreTarget);

        Settings settings = Settings.builder()
            .put(SslConfigSettings.SSL_KEYSTORE_FILEPATH.getKey(), keystoreLink.toString())
            .put(SslConfigSettings.SSL_RESOURCE_POLL_INTERVAL_NAME, "1")
            .build();

        final AtomicBoolean reloadCalled = new AtomicBoolean(false);
        SslContextProviderService sslContextProviderService = new SslContextProviderService(
            settings,
            THREAD_POOL,
            new SslContextProvider() {

                    @Override
                    public SslContext getSslContext() {
                        return null;
                    }

                    @Override
                    public void reloadSslContext() {
                        reloadCalled.set(true);
                    }
                }
        );
        sslContextProviderService.start();

        Files.write(dataTarget06.resolve("keystore.jks"), List.of("version2"));
        Files.deleteIfExists(dataLink);
        Files.createSymbolicLink(dataLink, dataTarget06);
        Files.deleteIfExists(dataTarget05.resolve("keystore.jks"));

        assertBusy(() -> {
            assertThat(reloadCalled.get(), is(true));
        });
    }
}
