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

package io.crate.auth;

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.protocols.ssl.SslConfigSettings;
import io.crate.testing.UseJdbc;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.KeyStore;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.Matchers.containsString;

/**
 * Additional tests to the SSL tests in {@link AuthenticationIntegrationTest} where SSL
 * support is disabled. In this test we have SSL support.
 */
@UseJdbc(value = 1)
public class AuthenticationWithSSLIntegrationTest extends SQLTransportIntegrationTest {

    private static File trustStoreFile;
    private static File keyStoreFile;
    private static String defaultKeyStoreType = KeyStore.getDefaultType();

    public AuthenticationWithSSLIntegrationTest() {
        super(true);
    }

    @BeforeClass
    public static void beforeIntegrationTest() throws IOException {
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.jks");
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.jks");
        Security.setProperty("keystore.type", "jks");
    }

    @AfterClass
    public static void resetKeyStoreType() {
        Security.setProperty("keystore.type", defaultKeyStoreType);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SslConfigSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslConfigSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "serverKeyPassword")
            .put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "truststorePassword")
            .put("network.host", "127.0.0.1")
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config",
                "a",
                new String[]{"user", "method", "address"},
                new String[]{"crate", "trust", "127.0.0.1"})
            .put("auth.host_based.config",
                "e",
                new String[]{"user", HostBasedAuthentication.SSL.KEY},
                new String[]{"requiredssluser", HostBasedAuthentication.SSL.REQUIRED.VALUE})
            .put("auth.host_based.config",
                "f",
                new String[]{"user", HostBasedAuthentication.SSL.KEY},
                new String[]{"optionalssluser", HostBasedAuthentication.SSL.OPTIONAL.VALUE})
            .put("auth.host_based.config",
                "g",
                new String[]{"user", HostBasedAuthentication.SSL.KEY},
                new String[]{"neverssluser", HostBasedAuthentication.SSL.NEVER.VALUE})
            .put("auth.host_based.config.h.user", "localhost") // cert used in tests has CN=localhost, username needs to match
            .put("auth.host_based.config.h.method", "cert")
            .build();
    }

    @Test
    @SuppressWarnings("EmptyTryBlock")
    public void checkSslConfigOption() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.createStatement().execute("CREATE USER requiredssluser");
            conn.createStatement().execute("CREATE USER optionalssluser");
            conn.createStatement().execute("CREATE USER neverssluser");
            conn.createStatement().execute("GRANT DQL TO requiredssluser, optionalssluser, neverssluser");
        }

        // We have SSL available in the following tests:

        properties.setProperty("user", "optionalssluser");
        try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {}

        try {
            properties.setProperty("user", "neverssluser");
            try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {}
            fail("User was able to use SSL although HBA config had requireSSL=never set.");
        } catch (PSQLException e) {
            assertThat(e.getMessage(), containsString("FATAL: No valid auth.host_based entry found"));
        }

        properties.setProperty("user", "requiredssluser");
        try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {}
    }

    @Test
    public void testClientCertAuthWithoutCert() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "localhost");
        properties.setProperty("ssl", "true");
        expectedException.expectMessage("Client certificate authentication failed for user \"localhost\"");
        try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {}
    }


    @After
    public void dropUsers() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.createStatement().execute("DROP USER IF EXISTS arthur");
            conn.createStatement().execute("DROP USER IF EXISTS requiredsslruser");
            conn.createStatement().execute("DROP USER IF EXISTS optionalssluser");
            conn.createStatement().execute("DROP USER IF EXISTS neverssluser");
            conn.createStatement().execute("DROP USER IF EXISTS localhost");
        }
    }

    private static File getAbsoluteFilePathFromClassPath(final String fileNameFromClasspath) throws IOException {
        final URL fileUrl = AuthenticationWithSSLIntegrationTest.class.getClassLoader().getResource(fileNameFromClasspath);
        if (fileUrl == null) {
            throw new FileNotFoundException("Resource was not found: " + fileNameFromClasspath);
        }
        return new File(URLDecoder.decode(fileUrl.getFile(), "UTF-8"));
    }
}
