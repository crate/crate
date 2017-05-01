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

package io.crate.operation.user;

import io.crate.action.sql.SQLActionException;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UserManagementIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testCreateUser() throws Exception {
        execute("create user fridolin");
        assertThat(response.rowCount(), is(1L));
        // it's not possible to create the same user again
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(containsString("User already exists"));
        execute("create user fridolin");
    }

    @Test
    public void testSysUsersTableColumns() throws Exception {
        // The sys users table contains two columns, name and superuser
        execute("select column_name, data_type from information_schema.columns where table_name='users' and table_schema='sys'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("name| string\n" +
                                                                    "superuser| boolean\n"));
    }

    @Test
    public void testSysUsersTableDefaultUser() throws Exception {
        // The sys.users table always contains the superuser crate
        execute("select name, superuser from sys.users");
        assertThat(TestingHelpers.printedTable(response.rows()), is("crate| true\n"));
    }

    @Test
    public void testDropUser() throws Exception {
        execute("create user ford");
        sleep(5L); // TODO: replace sleep with wait method when user table is merged
        execute("drop user ford");
        // TODO: that no conflict exception is thrown, shows that the user got deleted successfully.
        // Replace this with a real check as soon as user table is merged
        execute("create user ford");
    }

    @Test
    public void testDropUserIfExists() throws Exception {
        execute("drop user if exists ford");
        assertThat(response.rowCount(), is(0L));
    }
}
