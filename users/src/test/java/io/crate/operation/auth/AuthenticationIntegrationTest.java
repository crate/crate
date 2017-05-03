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
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

@UseJdbc(value = 1)
public class AuthenticationIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("network.host", "127.0.0.1")
            .put(Settings.builder()
                .put("auth.host_based.a.user", "crate")
                .put("auth.host_based.a.method", "trust")
                .put("auth.host_based.a.address", "127.0.0.1")
                .build())
            .put(Settings.builder()
                .put("auth.host_based.b.user", "cr8")
                .put("auth.host_based.b.method", "trust")
                .put("auth.host_based.b.address", "0.0.0.0/0")
                .build())
            .put(Settings.builder()
                .put("auth.host_based.c.user", "foo")
                .put("auth.host_based.c.method", "fake")
                .put("auth.host_based.c.address", "127.0.0.1/32")
                .build())
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
        expectedException.expectMessage("FATAL: No valid auth.host_based entry found for host \"127.0.0.1\", user \"me\", schema \"doc\"");
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
        expectedException.expectMessage("FATAL: No valid auth.host_based entry found for host \"127.0.0.1\", user \"foo\", schema \"doc\"");
        Properties properties = new Properties();
        properties.setProperty("user", "foo");
        DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties);
    }

}
