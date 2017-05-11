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

package io.crate.operation.auth;

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.shade.org.postgresql.util.PSQLException;
import io.crate.testing.UseJdbc;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@UseJdbc(value = 1)
public class AuthenticationIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("network.host", "127.0.0.1")
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config",
                "a", new String[]{"user", "method", "address"}, new String[]{"crate", "trust", "127.0.0.1"})
            .put("auth.host_based.config",
                "b", new String[]{"user", "method", "address"}, new String[]{"cr8", "trust", "0.0.0.0/0"})
            .put("auth.host_based.config",
                "c", new String[]{"user", "method", "address"}, new String[]{"foo", "fake", "127.0.0.1/32"})
            .put("auth.host_based.config",
                "d", new String[]{"user", "method", "address"}, new String[]{"arthur", "trust", "127.0.0.1"})
            .build();
    }

    @Test
    public void testValidCrateUser() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties);
    }

    @Test
    public void testInvalidUser() throws Exception {
        expectedException.expect(PSQLException.class);
        expectedException.expectMessage("FATAL: No valid auth.host_based entry found for host \"127.0.0.1\", user \"me\"");
        Properties properties = new Properties();
        properties.setProperty("user", "me");
        Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties);
        conn.close();
    }

    @Test
    public void testUserInHbaThatDoesNotExist() throws Exception {
        expectedException.expect(PSQLException.class);
        expectedException.expectMessage("FATAL: trust authentication failed for user \"cr8\"");
        Properties properties = new Properties();
        properties.setProperty("user", "cr8");
        DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties);
    }

    @Test
    public void testInvalidAuthenticationMethod() throws Exception {
        expectedException.expect(PSQLException.class);
        expectedException.expectMessage("FATAL: No valid auth.host_based entry found for host \"127.0.0.1\", user \"foo\"");
        Properties properties = new Properties();
        properties.setProperty("user", "foo");
        DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties);
    }

    @Test
    public void testAuthenticationWithCreatedUser() throws Exception {
        // create a user with the crate user
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.createStatement().execute("CREATE USER arthur");
        }
        // connection with user arthur is possible
        properties.setProperty("user", "arthur");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            assertThat(conn, is(notNullValue()));
        };
    }

    @After
    public void dropUsers() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.createStatement().execute("DROP USER IF EXISTS arthur");
        }
    }
}
