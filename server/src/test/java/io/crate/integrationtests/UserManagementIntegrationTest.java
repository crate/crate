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

package io.crate.integrationtests;

import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Test;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UserManagementIntegrationTest extends BaseUsersIntegrationTest {

    private void assertUserIsCreated(String userName) throws Exception {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.users where name = ?",
            new Object[]{userName});
        assertThat(response.rows()[0][0], is(1L));
    }

    private void assertUserDoesntExist(String userName) throws Exception {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.users where name = ?",
            new Object[]{userName});
        assertThat(response.rows()[0][0], is(0L));
    }

    @After
    public void dropAllUsers() throws Exception {
        // clean all created users
        executeAsSuperuser("SELECT name FROM sys.users WHERE superuser = FALSE");
        for (Object[] objects : response.rows()) {
            String user = (String) objects[0];
            executeAsSuperuser("DROP user " + user);
        }
    }

    @Test
    public void testCreateUser() throws Exception {
        executeAsSuperuser("create user trillian");
        assertThat(response.rowCount(), is(1L));
        assertUserIsCreated("trillian");
    }

    @Test
    public void testSysUsersTableColumns() throws Exception {
        // The sys users table contains two columns, name and superuser
        executeAsSuperuser("select column_name, data_type from information_schema.columns where table_name='users' and table_schema='sys'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("name| text\n" +
                                                                    "password| text\n" +
                                                                    "superuser| boolean\n"));
    }

    @Test
    public void testSysUsersTableDefaultUser() throws Exception {
        // The sys.users table always contains the superuser crate
        executeAsSuperuser("select name, password, superuser from sys.users where name = 'crate'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("crate| NULL| true\n"));
    }

    @Test
    public void testSysUsersTable() throws Exception {
        executeAsSuperuser("CREATE USER arthur");
        assertUserIsCreated("arthur");
        executeAsSuperuser("CREATE USER ford WITH (password = 'foo')");
        assertUserIsCreated("ford");
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        // Every created user is not a superuser
        assertThat(TestingHelpers.printedTable(response.rows()), is("arthur| NULL| false\n" +
                                                                    "ford| ********| false\n"));
    }

    @Test
    public void testCreateUserWithPasswordExpression() throws Exception {
        executeAsSuperuser("CREATE USER arthur WITH (password = substr(?, 3))", new Object[]{"**password"});
        assertUserIsCreated("arthur");
    }

    @Test
    public void testAlterUserPassword() throws Exception {
        executeAsSuperuser("CREATE USER arthur");
        assertUserIsCreated("arthur");
        executeAsSuperuser("CREATE USER ford WITH (password = ?)", new Object[]{"foo"});
        assertUserIsCreated("ford");
        executeAsSuperuser("ALTER USER arthur SET (password = ?)", new Object[]{"pass"});
        executeAsSuperuser("ALTER USER ford SET (password = ?)", new Object[]{"pass"});
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        assertThat(TestingHelpers.printedTable(response.rows()), is("arthur| ********| false\n" +
                                                                    "ford| ********| false\n"));
    }

    @Test
    public void testAlterUserResetPassword() throws Exception {
        executeAsSuperuser("CREATE USER arthur WITH (password = 'foo')");
        assertUserIsCreated("arthur");
        executeAsSuperuser("ALTER USER arthur SET (password = null)");
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        assertThat(TestingHelpers.printedTable(response.rows()), is("arthur| NULL| false\n"));
    }

    @Test
    public void testAlterNonExistingUserThrowsException() throws Exception {
        assertThrows(() -> executeAsSuperuser("alter user unknown_user set (password = 'unknown')"),
                     isSQLError(is("User 'unknown_user' does not exist"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                40410));
    }

    @Test
    public void testDropUser() throws Exception {
        executeAsSuperuser("create user ford");
        assertUserIsCreated("ford");
        executeAsSuperuser("drop user ford");
        assertUserDoesntExist("ford");
    }

    @Test
    public void testDropUserIfExists() throws Exception {
        executeAsSuperuser("drop user if exists not_exist_if_exists");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testDropUserUnAuthorized() throws Exception {
        assertThrows(() -> executeAsNormalUser("drop user ford"),
                     isSQLError(is("Missing 'AL' privilege for user 'normal'"),
                                INTERNAL_ERROR,
                                UNAUTHORIZED,
                                4011));
    }

    @Test
    public void testCreateUserUnAuthorized() throws Exception {
        assertThrows(() -> executeAsNormalUser("create user ford"),
                     isSQLError(is("Missing 'AL' privilege for user 'normal'"),
                                INTERNAL_ERROR,
                                UNAUTHORIZED,
                                4011));
    }

    @Test
    public void testCreateNormalUserUnAuthorized() throws Exception {
        assertThrows(() -> executeAsNormalUser("create user ford"),
                     isSQLError(is("Missing 'AL' privilege for user 'normal'"),
                                INTERNAL_ERROR,
                                UNAUTHORIZED,
                                4011));
    }

    @Test
    public void test_create_user_as_regular_user_with_al_privileges() throws Exception {
        executeAsSuperuser("CREATE USER trillian");
        executeAsSuperuser("GRANT AL TO trillian");
        executeAs("CREATE USER arthur", "trillian");
        assertUserIsCreated("arthur");
        executeAs("DROP USER arthur", "trillian");
        assertUserDoesntExist("arthur");
    }

    @Test
    public void testCreateExistingUserThrowsException() throws Exception {
        executeAsSuperuser("create user ford_exists");
        assertUserIsCreated("ford_exists");

        assertThrows(() -> executeAsSuperuser("create user ford_exists"),
                     isSQLError(is("User 'ford_exists' already exists"),
                                INTERNAL_ERROR,
                                CONFLICT,
                                4099));
    }

    @Test
    public void testDropNonExistingUserThrowsException() throws Exception {
        assertThrows(() -> executeAsSuperuser("drop user not_exists"),
                     isSQLError(is("User 'not_exists' does not exist"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                40410));
    }

    @Test
    public void testDropSuperUserThrowsException() throws Exception {
        assertThrows(() -> executeAsSuperuser("drop user crate"),
                     isSQLError(is("Cannot drop a superuser 'crate'"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4004));
    }
}
