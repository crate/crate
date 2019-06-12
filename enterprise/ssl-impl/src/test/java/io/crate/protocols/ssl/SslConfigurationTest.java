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
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.KeyManager;
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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SslConfigurationTest extends CrateUnitTest {

    private static final String KEYSTORE_PASSWORD = "keystorePassword";
    private static final String KEYSTORE_KEY_PASSWORD = "serverKeyPassword";
    private static final String TRUSTSTORE_PASSWORD = "truststorePassword";

    private static final String ROOT_CA_ALIAS = "theCAroot";

    private static File trustStoreFile;
    private static File keyStoreFile;

    private String originalKeyStoreType;

    @BeforeClass
    public static void beforeTests() throws IOException {
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.jks");
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.jks");
    }

    @Before
    public void setKeyStoreType() throws Exception {
        originalKeyStoreType = Security.getProperty("keystore.type");
        Security.setProperty("keystore.type", "jks");
    }

    @After
    public void resetKeyStoreType() throws Exception {
        Security.setProperty("keystore.type", originalKeyStoreType);
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
    public void testKeyStoreLoading() throws Exception {
        KeyStore keyStore = SslConfiguration.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());
        assertThat(keyStore.getType(), is("jks"));
        assertThat(keyStore.getCertificate(ROOT_CA_ALIAS), notNullValue());

        KeyManager[] keyManagers = SslConfiguration.createKeyManagers(keyStore, KEYSTORE_KEY_PASSWORD.toCharArray());
        assertThat(keyManagers.length, is(1));
    }

    @Test
    public void testKeyStoreLoadingFailWrongPassword() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Keystore was tampered with, or password was incorrect");

        SslConfiguration.loadKeyStore(keyStoreFile.getAbsolutePath(), "wrongpassword".toCharArray());
    }

    @Test
    public void testKeyStoreLoadingFailWrongKeyPassword() throws Exception {
        KeyStore keyStore = SslConfiguration.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        expectedException.expect(UnrecoverableKeyException.class);
        expectedException.expectMessage("Cannot recover key");
        SslConfiguration.createKeyManagers(keyStore, "wrongpassword".toCharArray());
    }

    @Test
    public void testExportRootCertificates() throws Exception {
        KeyStore keyStore = SslConfiguration.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        X509Certificate[] certificates = SslConfiguration.getRootCertificates(keyStore);
        assertThat(certificates.length, is(1));

        assertThat(certificates[0].getIssuerDN().getName(), containsString("CN=*.crate.io"));
        assertThat(certificates[0].getNotAfter().getTime(), is(4651463625000L));
    }

    @Test
    public void testExportServerCertChain() throws Exception {
        KeyStore keyStore = SslConfiguration.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());

        X509Certificate[] certificates = SslConfiguration.getCertificateChain(keyStore);

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
        KeyStore keyStore = SslConfiguration.loadKeyStore(keyStoreFile.getAbsolutePath(), KEYSTORE_PASSWORD.toCharArray());
        PrivateKey privateKey = SslConfiguration.getPrivateKey(keyStore, KEYSTORE_KEY_PASSWORD.toCharArray());
        assertThat(privateKey, Matchers.notNullValue());
    }

    public static File getAbsoluteFilePathFromClassPath(final String fileNameFromClasspath) throws IOException {
        final URL fileUrl = SslConfigurationTest.class.getClassLoader().getResource(fileNameFromClasspath);
        if (fileUrl == null) {
            throw new FileNotFoundException("Resource was not found: " + fileNameFromClasspath);
        }
        return new File(URLDecoder.decode(fileUrl.getFile(), StandardCharsets.UTF_8));
    }
}
