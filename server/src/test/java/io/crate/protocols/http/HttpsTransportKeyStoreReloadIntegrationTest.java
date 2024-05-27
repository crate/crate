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
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.integrationtests.SQLHttpIntegrationTest;
import io.crate.protocols.ssl.SslSettings;
import io.crate.testing.UseJdbc;
import io.netty.handler.ssl.util.SelfSignedCertificate;

@UseJdbc(value = 0)
@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class HttpsTransportKeyStoreReloadIntegrationTest extends SQLHttpIntegrationTest {

    private static final char[] EMPTY_PASS = new char[]{};

    private static SelfSignedCertificate trustedCert;
    private static File keyStoreFile;
    private static File trustStoreFile;

    public HttpsTransportKeyStoreReloadIntegrationTest() {
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
        trustStoreFile = createTempFile().toFile();

        updateKeyStore(
            keyStoreFile,
            store -> {
                SelfSignedCertificate fakeCert = new SelfSignedCertificate();
                store.setKeyEntry(
                    "key",
                    fakeCert.key(),
                    EMPTY_PASS,
                    new Certificate[]{fakeCert.cert()});
            });

        updateKeyStore(
            trustStoreFile,
            keyStore -> keyStore.setCertificateEntry("cert", trustedCert.cert()));

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
        try (CloseableHttpResponse ignored = post("{\"stmt\": \"select 'sslWorks'\"}")) {
            fail("Certificates in the trusted store signed with unknown key");
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
            try (CloseableHttpResponse response = post("{\"stmt\": \"select 'sslWorks'\"}")) {
                assertThat(response).isNotNull();
                assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
                String result = EntityUtils.toString(response.getEntity());
                assertThat(result, containsString("\"rowcount\":1"));
                assertThat(result, containsString("sslWorks"));
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }, 20, TimeUnit.SECONDS);
    }
}
