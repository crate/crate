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

package io.crate.auth;

import static io.crate.protocols.postgres.PGErrorStatus.INVALID_AUTHORIZATION_SPECIFICATION;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import io.crate.protocols.ssl.SslSettings;
import io.crate.testing.Asserts;
import io.crate.testing.UseJdbc;

/**
 * Additional tests to the SSL tests in {@link AuthenticationIntegrationTest} where SSL
 * support is disabled. In this test we have SSL support.
 */
@UseJdbc(value = 1)
public class AuthenticationWithSSLIntegrationTest extends IntegTestCase {

    private static File trustStoreFile;
    private static File keyStoreFile;

    public AuthenticationWithSSLIntegrationTest() {
        super(true);
    }

    @BeforeClass
    public static void beforeIntegrationTest() throws IOException {
        keyStoreFile = getAbsoluteFilePathFromClassPath("keystore.pcks12");
        trustStoreFile = getAbsoluteFilePathFromClassPath("truststore.pcks12");
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SslSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "keystorePassword")
            .put(SslSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile.getAbsolutePath())
            .put(SslSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "keystorePassword")
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
        try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
        }

        try {
            properties.setProperty("user", "neverssluser");
            try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            }
            fail("User was able to use SSL although HBA config had requireSSL=never set.");
        } catch (PSQLException e) {
            assertThat(e.getMessage()).contains("FATAL: No valid auth.host_based entry found");
        }

        properties.setProperty("user", "requiredssluser");
        try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
        }
    }

    @Test
    public void testClientCertAuthWithoutCert() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "localhost");
        properties.setProperty("ssl", "true");

        Asserts.assertSQLError(
            () -> {
                try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
                }
            })
            .isExactlyInstanceOf(PSQLException.class)
            .hasPGError(INVALID_AUTHORIZATION_SPECIFICATION)
            // <=5.7.1 used to fail with different message "authentication failed".
            // After 5.7.2 we take connection properties into account (client cert, password or token headers) for matching an auth method.
            // Thus, a connection without client cert doesn't even qualify as matching anymore
            .hasMessageContaining("No valid auth.host_based entry found for host \"127.0.0.1\", user \"localhost\". Did you enable TLS in your client?");
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

    static File getAbsoluteFilePathFromClassPath(final String fileNameFromClasspath) throws IOException {
        final URL fileUrl = AuthenticationWithSSLIntegrationTest.class.getClassLoader().getResource(fileNameFromClasspath);
        if (fileUrl == null) {
            throw new FileNotFoundException("Resource was not found: " + fileNameFromClasspath);
        }
        return new File(URLDecoder.decode(fileUrl.getFile(), "UTF-8"));
    }
}
