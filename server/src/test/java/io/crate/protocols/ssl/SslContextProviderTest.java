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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.handler.ssl.SslContext;

public class SslContextProviderTest extends ESTestCase {

    private static final String KEYSTORE_PASSWORD = "keystorePassword";
    private static final String KEYSTORE_KEY_PASSWORD = "serverKeyPassword";
    private static final String TRUSTSTORE_PASSWORD = "truststorePassword";
    private static final String ROOT_CA_ALIAS = "theCAroot";

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
    public void testClassLoadingWithInvalidConfiguration() {
        // empty ssl configuration which is invalid
        Settings settings = Settings.builder()
            .put(SslSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslSettings.SSL_PSQL_ENABLED.getKey(), true)
            .build();
        expectedException.expect(SslConfigurationException.class);
        expectedException.expectMessage("Failed to build SSL configuration");
        var sslContextProvider = new SslContextProvider(settings);
        sslContextProvider.getServerContext();
    }

    @Test
    public void testClassLoadingWithValidConfiguration() {
        Settings settings = Settings.builder()
            .put(SslSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "truststorePassword")
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "serverKeyPassword")
            .build();
        var sslContextProvider = new SslContextProvider(settings);
        SslContext sslContext = sslContextProvider.getServerContext();
        assertThat(sslContext, instanceOf(SslContext.class));
        assertThat(sslContext.isServer(), is(true));
        assertThat(sslContext.cipherSuites(), not(empty()));
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
        SslContext sslContext = new SslContextProvider(settings).getServerContext();
        assertThat(sslContext.isServer(), is(true));
        assertThat(sslContext.cipherSuites(), not(empty()));
        // check that we don't offer NULL ciphers which do not encrypt
        assertThat(sslContext.cipherSuites(), not(hasItem(containsString("NULL"))));

        assertThat(defaultSSLContext, Matchers.sameInstance(SSLContext.getDefault()));
    }

    @Test
    public void testKeyStoreLoading() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());
        assertThat(keyStore.getType(), is("jks"));
        assertThat(keyStore.getCertificate(ROOT_CA_ALIAS), notNullValue());

        KeyManager[] keyManagers = SslContextProvider.createKeyManagers(keyStore, KEYSTORE_KEY_PASSWORD.toCharArray());
        assertThat(keyManagers.length, is(1));
    }

    @Test
    public void testKeyStoreLoadingFailWrongPassword() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Keystore was tampered with, or password was incorrect");

        SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), "wrongpassword".toCharArray());
    }

    @Test
    public void testKeyStoreLoadingFailWrongKeyPassword() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        expectedException.expect(UnrecoverableKeyException.class);
        expectedException.expectMessage("Cannot recover key");
        SslContextProvider.createKeyManagers(keyStore, "wrongpassword".toCharArray());
    }

    @Test
    public void testExportRootCertificates() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        X509Certificate[] certificates = SslContextProvider.getRootCertificates(keyStore);
        assertThat(certificates.length, is(1));

        assertThat(certificates[0].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(certificates[0].getNotAfter().getTime(), is(4651463625000L));
    }

    @Test
    public void testExportServerCertChain() throws Exception {
        KeyStore keyStore = SslContextProvider.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        X509Certificate[] certificates = SslContextProvider.getCertificateChain(keyStore);

        assertThat(certificates.length, is(4));
        assertThat(certificates[0].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(certificates[1].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(certificates[2].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(certificates[3].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(certificates[0].getSubjectDN().getName(), containsString("CN=localhost"));
        assertThat(certificates[0].getSubjectDN().getName(), containsString("OU=Testing Department"));
        assertThat(certificates[0].getSubjectDN().getName(), containsString("L=San Francisco"));
        assertThat(certificates[1].getSubjectDN().getName(), containsString("CN=*.crate.io"));
        assertThat(certificates[1].getSubjectDN().getName(), containsString("OU=Cryptography Department"));
        assertThat(certificates[1].getSubjectDN().getName(), containsString("L=Dornbirn"));
        assertThat(certificates[2].getSubjectDN().getName(), containsString("CN=ssl.crate.io"));
        assertThat(certificates[2].getSubjectDN().getName(), containsString("OU=Cryptography Department"));
        assertThat(certificates[2].getSubjectDN().getName(), containsString("L=Berlin"));
        assertThat(certificates[3].getSubjectDN().getName(), containsString("CN=*.crate.io"));
        assertThat(certificates[3].getSubjectDN().getName(), containsString("OU=Cryptography Department"));
        assertThat(certificates[3].getSubjectDN().getName(), containsString("L=Dornbirn"));
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
