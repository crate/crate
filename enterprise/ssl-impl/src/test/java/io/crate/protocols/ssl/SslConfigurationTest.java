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

import io.crate.test.integration.CrateUnitTest;
import io.netty.handler.ssl.SslContext;
import org.elasticsearch.common.settings.Settings;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;

import static io.crate.protocols.ssl.SslConfiguration.KeyStoreSettings;
import static io.crate.protocols.ssl.SslConfiguration.TrustStoreSettings;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SslConfigurationTest extends CrateUnitTest {

    private static final String KEYSTORE_PASSWORD = "keystorePassword";
    private static final String KEYSTORE_KEY_PASSWORD = "serverKeyPassword";
    private static final String TRUSTSTORE_PASSWORD = "truststorePassword";

    private static final String ROOT_CA_ALIAS = "theCAroot";

    private static File trustStoreFile;
    private static File keyStoreFile;

    @BeforeClass
    public static void beforeTests() throws IOException {
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.jks");
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.jks");
    }

    @Test
    public void testSslContextCreation() {
        Settings settings = Settings.builder()
            .put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH_SETTING_NAME, trustStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD_SETTING_NAME, TRUSTSTORE_PASSWORD)
            .put(SslConfigSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, KEYSTORE_PASSWORD)
            .put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, KEYSTORE_KEY_PASSWORD)
        .build();
        SslContext sslContext = SslConfiguration.buildSslContext(settings);
        assertThat(sslContext.isServer(), is(true));
        assertThat(sslContext.cipherSuites(), not(empty()));
        // check that we don't offer NULL ciphers which do not encrypt
        assertThat(sslContext.cipherSuites(), not(hasItem(containsString("NULL"))));
    }

    @Test
    public void testTrustStoreLoading() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH_SETTING_NAME, trustStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD_SETTING_NAME, TRUSTSTORE_PASSWORD);

        TrustStoreSettings trustStoreSettings =
            TrustStoreSettings.tryLoad(settingsBuilder.build()).orElse(null);

        assertThat(trustStoreSettings, is(notNullValue()));
        assertThat(trustStoreSettings.trustManagers.length, is(1));
        assertThat(trustStoreSettings.keyStore.getType(), is("jks"));
    }

    @Test
    public void testTrustStoreOptional() throws Exception {
        TrustStoreSettings trustStoreSettings =
            TrustStoreSettings.tryLoad(Settings.EMPTY).orElse(null);
        assertThat(trustStoreSettings, is(nullValue()));
    }

    @Test
    public void testTrustStoreLoadingFail() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Keystore was tampered with, or password was incorrect");

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH_SETTING_NAME, trustStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD_SETTING_NAME, "wrongpassword");

        TrustStoreSettings.tryLoad(settingsBuilder.build());
    }

    @Test
    public void testKeyStoreLoading() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, KEYSTORE_PASSWORD);
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, KEYSTORE_KEY_PASSWORD);

        KeyStoreSettings keyStoreSettings = new KeyStoreSettings(settingsBuilder.build());
        assertThat(keyStoreSettings.keyManagers.length, is(1));
        assertThat(keyStoreSettings.keyStore.getType(), is("jks"));
        assertThat(keyStoreSettings.keyStore.getCertificate(ROOT_CA_ALIAS), notNullValue());
    }

    @Test
    public void testKeyStoreLoadingFailWrongPassword() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Keystore was tampered with, or password was incorrect");

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, "wrongpassword");
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, KEYSTORE_KEY_PASSWORD);

        new KeyStoreSettings(settingsBuilder.build());
    }

    @Test
    public void testKeyStoreLoadingFailWrongKeyPassword() throws Exception {
        expectedException.expect(UnrecoverableKeyException.class);
        expectedException.expectMessage("Cannot recover key");

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, KEYSTORE_PASSWORD);
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, "wrongpassword");

        KeyStoreSettings ks = new KeyStoreSettings(settingsBuilder.build());
        assertThat(ks.exportDecryptedKey(), is(notNullValue()));
    }

    @Test
    public void testKeyStoreLoadingNoKeyPasswordFail() throws Exception {
        expectedException.expect(UnrecoverableKeyException.class);
        expectedException.expectMessage("Cannot recover key");

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, KEYSTORE_PASSWORD);
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, "wrongpassword");

        KeyStoreSettings ks = new KeyStoreSettings(settingsBuilder.build());
        assertThat(ks.exportDecryptedKey(), is(notNullValue()));
    }

    @Test
    public void testExportRootCertificates() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH_SETTING_NAME, trustStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD_SETTING_NAME, TRUSTSTORE_PASSWORD);
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, KEYSTORE_PASSWORD);
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, KEYSTORE_KEY_PASSWORD);

        KeyStoreSettings keyStoreSettings = new KeyStoreSettings(settingsBuilder.build());

        TrustStoreSettings trustStoreSettings =
            TrustStoreSettings.tryLoad(settingsBuilder.build()).orElse(null);

        assertThat(trustStoreSettings, is(notNullValue()));
        X509Certificate[] x509Certificates = keyStoreSettings.exportRootCertificates(
            trustStoreSettings.exportRootCertificates());

        assertEquals(2, x509Certificates.length);
        assertThat(x509Certificates[0].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(x509Certificates[0].getNotAfter().getTime(), is(4651463625000L));
        assertThat(x509Certificates[1].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(x509Certificates[1].getNotAfter().getTime(), is(4651463625000L));
    }

    @Test
    public void testExportServerCertChain() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, KEYSTORE_PASSWORD);
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, KEYSTORE_KEY_PASSWORD);

        KeyStoreSettings keyStoreSettings = new KeyStoreSettings(settingsBuilder.build());

        X509Certificate[] x509Certificates = keyStoreSettings.exportServerCertChain();

        assertThat(x509Certificates.length, is(4));
        assertThat(x509Certificates[0].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(x509Certificates[1].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(x509Certificates[2].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(x509Certificates[3].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(x509Certificates[0].getSubjectDN().getName(), containsString("CN=localhost"));
        assertThat(x509Certificates[0].getSubjectDN().getName(), containsString("OU=Testing Department"));
        assertThat(x509Certificates[0].getSubjectDN().getName(), containsString("L=San Francisco"));
        assertThat(x509Certificates[1].getSubjectDN().getName(), containsString("CN=*.crate.io"));
        assertThat(x509Certificates[1].getSubjectDN().getName(), containsString("OU=Cryptography Department"));
        assertThat(x509Certificates[1].getSubjectDN().getName(), containsString("L=Dornbirn"));
        assertThat(x509Certificates[2].getSubjectDN().getName(), containsString("CN=ssl.crate.io"));
        assertThat(x509Certificates[2].getSubjectDN().getName(), containsString("OU=Cryptography Department"));
        assertThat(x509Certificates[2].getSubjectDN().getName(), containsString("L=Berlin"));
        assertThat(x509Certificates[3].getSubjectDN().getName(), containsString("CN=*.crate.io"));
        assertThat(x509Certificates[3].getSubjectDN().getName(), containsString("OU=Cryptography Department"));
        assertThat(x509Certificates[3].getSubjectDN().getName(), containsString("L=Dornbirn"));
    }

    @Test
    public void testExportDecryptedKey() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_FILEPATH_SETTING_NAME, keyStoreFile.getAbsolutePath());
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_PASSWORD_SETTING_NAME, KEYSTORE_PASSWORD);
        settingsBuilder.put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, KEYSTORE_KEY_PASSWORD);

        KeyStoreSettings keyStoreSettings = new KeyStoreSettings(settingsBuilder.build());

        PrivateKey privateKey = keyStoreSettings.exportDecryptedKey();

        assertNotNull(privateKey);
    }

    public static File getAbsoluteFilePathFromClassPath(final String fileNameFromClasspath) throws IOException {
        final URL fileUrl = SslConfigurationTest.class.getClassLoader().getResource(fileNameFromClasspath);
        if (fileUrl == null) {
            throw new FileNotFoundException("Resource was not found: " + fileNameFromClasspath);
        }
        return new File(URLDecoder.decode(fileUrl.getFile(), "UTF-8"));
    }
}
