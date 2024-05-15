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

package io.crate.protocols.ssl;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.protocols.ssl.SslContextProviderService.FingerPrint;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

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
        Path resolvedKeystore = dataLink.resolve("keystore.jks");
        Files.write(resolvedKeystore, List.of("version1"));

        Path keystoreLink = tempDir.resolve("keystore.jks");
        Path keystoreTarget = tempDir.resolve("data/keystore.jks");
        Files.createSymbolicLink(keystoreLink, keystoreTarget);

        Settings settings = Settings.builder()
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keystoreLink.toString())
            .build();

        final AtomicBoolean reloadCalled = new AtomicBoolean(false);
        try (var sslContextProviderService = new SslContextProviderService(
            settings,
            THREAD_POOL,
            new SslContextProvider(Settings.EMPTY) {

                    @Override
                    public void reloadSslContext() {
                        reloadCalled.set(true);
                    }
                }
        )) {

            List<FingerPrint> filesToWatch = sslContextProviderService.createFilesToWatch();
            assertThat(filesToWatch).hasSize(1);

            FingerPrint fingerPrint = filesToWatch.get(0);
            long checksum = fingerPrint.checksum;

            Files.write(dataTarget06.resolve("keystore.jks"), List.of("version2"));
            assertThat(Files.deleteIfExists(dataLink))
                .as("dataLink file must exist")
                .isTrue();
            Files.createSymbolicLink(dataLink, dataTarget06);

            sslContextProviderService.pollForChanges(filesToWatch);
            assertThat(fingerPrint.checksum)
                .as("checksum must change after file changed")
                .isNotEqualTo(checksum);

            assertThat(reloadCalled.get())
                .as("reload must be called on SslContextProvider")
                .isTrue();

        }
    }
}
