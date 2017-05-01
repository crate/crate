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

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UserManagementIntegrationTest extends SQLTransportIntegrationTest {

    private void assertUserIsCreated(String userName) throws Exception {
        assertBusy(() -> {
            SQLResponse response = execute("select count(*) from sys.users where name = ?", new Object[]{userName});
            assertThat(response.rows()[0][0], is(1L));
        });
    }

    private void assertUserDoesntExist(String userName) throws Exception {
        assertBusy(() -> {
            SQLResponse response = execute("select count(*) from sys.users where name = ?", new Object[]{userName});
            assertThat(response.rows()[0][0], is(0L));
        });
    }

    @Test
    public void testCreateUser() throws Exception {
        execute("create user trillian");
        assertThat(response.rowCount(), is(1L));
        assertUserIsCreated("trillian");
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
        execute("select name, superuser from sys.users where name = 'crate'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("crate| true\n"));
    }

    @Test
    public void testSysUsersTable() throws Exception {
        execute("create user arthur");
        assertUserIsCreated("arthur");
        execute("select name, superuser from sys.users order by name limit 1");
        // Every created user is not a superuser
        assertThat(TestingHelpers.printedTable(response.rows()), is("arthur| false\n"));
    }

    @Test
    public void testDropUser() throws Exception {
        execute("create user ford");
        assertUserIsCreated("ford");
        execute("drop user ford");
        assertUserDoesntExist("ford");
    }

    @Test
    public void testDropUserIfExists() throws Exception {
        execute("drop user if exists ford");
        assertThat(response.rowCount(), is(0L));
    }
}
