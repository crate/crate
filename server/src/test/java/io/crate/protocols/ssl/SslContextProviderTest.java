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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.auth.Protocol;
import io.netty.handler.ssl.SslContext;

public class SslContextProviderTest extends ESTestCase {

    private static final String KEYSTORE_PASSWORD = "keystorePassword";
    private static final String KEYSTORE_KEY_PASSWORD = "keystorePassword";
    private static final String TRUSTSTORE_PASSWORD = "keystorePassword";
    private static final String ROOT_CA_ALIAS = "tmp/localhost";

    private static File trustStoreFile;
    private static File keyStoreFile;

    @BeforeClass
    public static void beforeTests() throws IOException {
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.pcks12");
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.pcks12");
    }


    @Test
    public void testClassLoadingWithInvalidConfiguration() {
        // empty ssl configuration which is invalid
        Settings settings = Settings.builder()
            .put(SslSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslSettings.SSL_PSQL_ENABLED.getKey(), true)
            .build();
        expectedException.expect(SslConfigurationException.class);
        expectedException.expectMessage("Failed to build SSL configuration");
        var sslContextProvider = new SslContextProvider(settings);
        sslContextProvider.getServerContext(Protocol.TRANSPORT);
    }

    @Test
    public void testClassLoadingWithValidConfiguration() {
        Settings settings = Settings.builder()
            .put(SslSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "keystorePassword")
            .build();
        var sslContextProvider = new SslContextProvider(settings);
        SslContext sslContext = sslContextProvider.getServerContext(Protocol.TRANSPORT);
        assertThat(sslContext, instanceOf(SslContext.class));
        assertThat(sslContext.isServer(), is(true));
        assertThat(sslContext.cipherSuites(), not(empty()));
    }

    @Test
    public void test_no_truststore_gets_defaults_certs() {
        TrustManager[] trustManagers = SslContextProvider.createTrustManagers(null);
        assertThat(SslContextProvider.getDefaultCertificates(trustManagers).length, greaterThan(0));
    }

    @Test
    public void test_netty_ssl_context_can_be_built_and_doesnt_override_default_context() throws Exception {
        var defaultSSLContext = SSLContext.getDefault();
        Settings settings = Settings.builder()
            .put(SslSettings.SSL_TRUSTSTORE_FILEPATH_SETTING_NAME, trustStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_TRUSTSTORE_PASSWORD_SETTING_NAME, TRUSTSTORE_PASSWORD)
            .put(SslSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, KEYSTORE_PASSWORD)
            .put(SslSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, KEYSTORE_KEY_PASSWORD)
            .build();
        SslContext sslContext = new SslContextProvider(settings).getServerContext(Protocol.TRANSPORT);
        assertThat(sslContext.isServer(), is(true));
        assertThat(sslContext.cipherSuites(), not(empty()));
        // check that we don't offer NULL ciphers which do not encrypt
        assertThat(sslContext.cipherSuites(), not(hasItem(containsString("NULL"))));

        assertThat(defaultSSLContext, Matchers.sameInstance(SSLContext.getDefault()));
    }

    @Test
    public void testKeyStoreLoading() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());
        assertThat(keyStore.getType(), is("pkcs12"));
        assertThat(keyStore.getCertificate(ROOT_CA_ALIAS), notNullValue());

        KeyManager[] keyManagers = SslContextProvider.createKeyManagers(keyStore, KEYSTORE_KEY_PASSWORD.toCharArray());
        assertThat(keyManagers.length, is(1));
    }

    @Test
    public void testKeyStoreLoadingFailWrongPassword() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("keystore password was incorrect");

        SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), "wrongpassword".toCharArray());
    }

    @Test
    public void testKeyStoreLoadingFailWrongKeyPassword() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        expectedException.expect(UnrecoverableKeyException.class);
        expectedException.expectMessage("Get Key failed");
        SslContextProvider.createKeyManagers(keyStore, "wrongpassword".toCharArray());
    }

    @Test
    public void testExportRootCertificates() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        X509Certificate[] certificates = SslContextProvider.getRootCertificates(keyStore);
        assertThat(certificates.length, is(1));

        assertThat(certificates[0].getIssuerDN().getName(), containsString("CN=myCA"));
        assertThat(certificates[0].getNotAfter().getTime(), is(1651658343000L));
    }

    @Test
    public void testExportServerCertChain() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        X509Certificate[] certificates = SslContextProvider.getCertificateChain(keyStore);

        assertThat(certificates.length, is(2));
        assertThat(certificates[0].getIssuerDN().getName(), containsString("CN=myCA, O=Dummy Company, L=Dummy Country, ST=Dummy State, C=AT"));
        assertThat(certificates[0].getSubjectDN().getName(), containsString("CN=localhost"));
        assertThat(certificates[1].getIssuerDN().getName(), containsString("CN=myCA, O=Dummy Company, L=Dummy Country, ST=Dummy State, C=AT"));
        assertThat(certificates[1].getSubjectDN().getName(), containsString("CN=myCA"));
    }

    @Test
    public void testExportDecryptedKey() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());
        PrivateKey privateKey = SslContextProvider.getPrivateKey(keyStore, KEYSTORE_KEY_PASSWORD.toCharArray());
        assertThat(privateKey, Matchers.notNullValue());
    }

    public static File getAbsoluteFilePathFromClassPath(final String fileNameFromClasspath) throws IOException {
        final URL fileUrl = SslContextProviderTest.class.getClassLoader().getResource(fileNameFromClasspath);
        if (fileUrl == null) {
            throw new FileNotFoundException("Resource was not found: " + fileNameFromClasspath);
        }
        return new File(URLDecoder.decode(fileUrl.getFile(), StandardCharsets.UTF_8));
    }
}
