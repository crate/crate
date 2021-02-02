/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
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
