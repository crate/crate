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

package io.crate.protocols.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.integrationtests.SQLHttpIntegrationTest;
import io.crate.protocols.ssl.SslSettings;
import io.crate.testing.UseJdbc;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

@UseJdbc(value = 0)
@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class HttpsTransportKeyStoreReloadIntegrationTest extends SQLHttpIntegrationTest {

    private static final char[] EMPTY_PASS = new char[]{};

    private static X509Bundle trustedCert;
    private static File keyStoreFile;
    private static File trustStoreFile;

    public HttpsTransportKeyStoreReloadIntegrationTest() throws Exception {
        super(true);
    }

    @BeforeClass
    public static void beforeTest() throws Exception {
        trustedCert = new CertificateBuilder()
            .subject("CN=localhost")
            .setIsCertificateAuthority(true)
            .buildSelfSigned();

        keyStoreFile = createTempFile().toFile();
        trustStoreFile = createTempFile().toFile();

        updateKeyStore(
            keyStoreFile,
            store -> {
                var fakeCert = new CertificateBuilder()
                    .subject("CN=localhost")
                    .setIsCertificateAuthority(true)
                    .buildSelfSigned();
                store.setKeyEntry(
                    "key",
                    fakeCert.getKeyPair().getPrivate(),
                    EMPTY_PASS,
                    new Certificate[]{fakeCert.getCertificate()});
            });

        updateKeyStore(
            trustStoreFile,
            keyStore -> keyStore.setCertificateEntry("cert", trustedCert.getCertificate()));

        System.setProperty("javax.net.ssl.trustStore", trustStoreFile.getAbsolutePath());
    }

    @AfterClass
    public static void afterTest() {
        System.clearProperty("javax.net.ssl.trustStore");
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
            .put(SslSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_RESOURCE_POLL_INTERVAL.getKey(), "2s")
            .build();
    }

    @Test
    public void testReloadSslContextOnKeyStoreChange() throws Throwable {
        post("{\"stmt\": \"select 'sslWorks'\"}");

        updateKeyStore(
            keyStoreFile,
            keyStore -> keyStore.setKeyEntry(
                "key",
                trustedCert.getKeyPair().getPrivate(),
                EMPTY_PASS,
                new Certificate[]{trustedCert.getCertificate()}));

        assertBusy(() -> {
            var response = post("{\"stmt\": \"select 'sslWorks'\"}");
            assertThat(response).isNotNull();
            assertThat(response.statusCode()).isEqualTo(200);
            String result = response.body();
            assertThat(result).contains("\"rowcount\":1");
            assertThat(result).contains("sslWorks");
        }, 20, TimeUnit.SECONDS);
    }
}
