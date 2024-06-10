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
import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import io.crate.testing.UseJdbc;
import io.netty.handler.codec.http.HttpHeaderNames;

@UseJdbc(value = 1)
public class AuthenticationIntegrationTest extends IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("network.host", "127.0.0.1")
            .put("http.host", "127.0.0.1")
            .put("http.cors.enabled", true)
            .put("http.cors.allow-origin", "*")
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config",
                "a", new String[]{"user", "method", "address"}, new String[]{"crate", "trust", "127.0.0.1"})
            .put("auth.host_based.config",
                "b", new String[]{"user", "method", "address"}, new String[]{"cr8", "trust", "0.0.0.0/0"})
            .put("auth.host_based.config",
                "c", new String[]{"user", "method", "address"}, new String[]{"foo", "fake", "127.0.0.1/32"})
            .put("auth.host_based.config",
                "d", new String[]{"user", "method", "address"}, new String[]{"arthur", "trust", "127.0.0.1"})
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
            .build();
    }

    @Test
    public void testOptionsRequestDoesNotRequireAuth() throws Exception {
        HttpServerTransport httpTransport = cluster().getInstance(HttpServerTransport.class);
        InetSocketAddress address = httpTransport.boundAddress().publishAddress().address();
        String uri = String.format(Locale.ENGLISH, "http://%s:%s/", address.getHostName(), address.getPort());
        HttpOptions request = new HttpOptions(uri);
        request.setHeader(HttpHeaderNames.AUTHORIZATION.toString(), "Basic QXJ0aHVyOkV4Y2FsaWJ1cg==");
        request.setHeader(HttpHeaderNames.ORIGIN.toString(), "http://example.com");
        request.setHeader(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD.toString(), "GET");
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            CloseableHttpResponse resp = httpClient.execute(request);
            assertThat(resp.getStatusLine().getReasonPhrase()).isEqualTo("OK");
        }
    }

    @Test
    public void testValidCrateUser() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        Connection connection = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties);
        connection.close();
    }

    @Test
    public void testInvalidUser() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "me");
        assertSQLError(() -> DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties))
            .isExactlyInstanceOf(PSQLException.class)
            .hasPGError(INVALID_AUTHORIZATION_SPECIFICATION)
            .hasMessageContaining("No valid auth.host_based entry found for host \"127.0.0.1\", user \"me\". Did you enable TLS in your client?");

    }

    @Test
    public void testUserInHbaThatDoesNotExist() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "cr8");
        assertSQLError(() -> DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties))
            .isExactlyInstanceOf(PSQLException.class)
            .hasPGError(INVALID_AUTHORIZATION_SPECIFICATION)
            .hasMessageContaining("trust authentication failed for user \"cr8\"");
    }

    @Test
    public void testInvalidAuthenticationMethod() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "foo");
        assertSQLError(() -> DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties))
            .isExactlyInstanceOf(PSQLException.class)
            .hasPGError(INVALID_AUTHORIZATION_SPECIFICATION)
            .hasMessageContaining("No valid auth.host_based entry found for host \"127.0.0.1\", user \"foo\". Did you enable TLS in your client?");
    }

    @Test
    public void testAuthenticationWithCreatedUser() throws Exception {
        // create a user with the crate user
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.createStatement().execute("CREATE USER arthur");
            conn.createStatement().execute("Grant DQL to arthur");
        }
        // connection with user arthur is possible
        properties.setProperty("user", "arthur");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            assertThat(conn).isNotNull();
        }
    }

    @Test
    public void checkSslConfigOption() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.createStatement().execute("CREATE USER requiredssluser");
            conn.createStatement().execute("CREATE USER optionalssluser");
            conn.createStatement().execute("CREATE USER neverssluser");
            conn.createStatement().execute("GRANT DQL to requiredssluser, optionalssluser, neverssluser");
        }

        // We don't have SSL available in the following tests:

        properties.setProperty("user", "optionalssluser");
        try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
        }

        properties.setProperty("user", "neverssluser");
        try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
        }

        try {
            properties.setProperty("user", "requiredssluser");
            try (Connection ignored = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            }
            fail("We were able to proceed without SSL although requireSSL=required was set.");
        } catch (PSQLException e) {
            assertThat(e.getMessage()).contains("FATAL: No valid auth.host_based entry found");
        }
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
        }
    }
}
