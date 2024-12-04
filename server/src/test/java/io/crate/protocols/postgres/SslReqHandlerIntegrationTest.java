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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.protocols.ssl.SslSettings;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import io.netty.handler.ssl.util.SelfSignedCertificate;


@UseJdbc(value = 1)
@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SslReqHandlerIntegrationTest extends IntegTestCase {

    private static final char[] EMPTY_PASS = new char[]{};

    private static SelfSignedCertificate trustedCert;
    private static File keyStoreFile;

    public SslReqHandlerIntegrationTest() {
        super(true);
    }

    public static void forceEnglishLocale() {
        // BouncyCastle is parsing date objects with the system locale while creating self-signed SSL certs
        // This fails for certain locales, e.g. 'ks'.
        // Until this is fixed, we force the english locale.
        // See also https://github.com/bcgit/bc-java/issues/405 (different topic, but same root cause)
        Locale.setDefault(Locale.ENGLISH);
    }

    @BeforeClass
    public static void beforeTest() throws Exception {
        forceEnglishLocale();

        trustedCert = new SelfSignedCertificate();
        keyStoreFile = createTempFile().toFile();

        updateKeyStore(
            keyStoreFile,
            store -> {
                SelfSignedCertificate fakeCert = new SelfSignedCertificate();
                store.setKeyEntry(
                    "key",
                    trustedCert.key(),
                    EMPTY_PASS,
                    new Certificate[]{fakeCert.cert()});
            });
    }

    private static void updateKeyStore(File keyStoreFile, CheckedConsumer<KeyStore, Exception> consumer)
        throws Exception {
        var keyStore = KeyStore.getInstance("JKS");
        try (var is = new FileInputStream(keyStoreFile)) {
            keyStore.load(is, EMPTY_PASS);
        } catch (Exception e) {
            keyStore.load(null);
        }
        consumer.accept(keyStore);
        try (var fs = new FileOutputStream(keyStoreFile)) {
            keyStore.store(fs, EMPTY_PASS);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SslSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_RESOURCE_POLL_INTERVAL.getKey(), "2s")
            .build();
    }

    @Test
    public void testReloadSslContextOnKeyStoreChange() throws Throwable {
        try {
            execute("select name from sys.nodes");
            fail("Unknown certificate in the certificate chain");
        } catch (Exception ignored) {
        }

        updateKeyStore(
            keyStoreFile,
            keyStore -> keyStore.setKeyEntry(
                "key",
                trustedCert.key(),
                EMPTY_PASS,
                new Certificate[]{trustedCert.cert()}));

        assertBusy(() -> {
            try {
                SQLResponse response = execute("select name from sys.nodes");
                assertThat(response.rowCount()).isEqualTo(1);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }, 20, TimeUnit.SECONDS);
    }
}
