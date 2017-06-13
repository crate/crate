/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import com.google.common.collect.ImmutableSet;
import io.crate.action.sql.Option;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.user.Privilege;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManager;
import io.crate.operation.user.UserManagerService;
import io.crate.settings.SharedSettings;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.SQLTransportExecutor.DEFAULT_SOFT_LIMIT;
import static org.hamcrest.core.Is.is;

/**
 * This suite requires no client or dedicated master nodes as otherwise the lookup for the node with enterprise.license
 * turned off won't work.
 */
@ESIntegTestCase.ClusterScope(numClientNodes = 0, supportsDedicatedMasters = false, minNumDataNodes = 2)
public class SysPrivilegesITest extends SQLTransportIntegrationTest {

    private static final Set<Privilege> PRIVILEGES = new HashSet<>(Arrays.asList(
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"),
        new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate")));
    private static final List<String> USERNAMES = Arrays.asList("Ford", "Arthur");

    private UserManager userManager;

    private SQLOperations.Session createSuperUserSession() {
        return createSuperUserSession(true);
    }

    private SQLOperations.Session createSuperUserSession(boolean enterpriseEnabled) {
        String nodeName = internalCluster().getNodeNames()[0];
        if (enterpriseEnabled == false) {
            nodeName = internalCluster().getNodeNames()[1];
        }
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, nodeName);
        return sqlOperations.createSession(null, UserManagerService.CRATE_USER, Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    private SQLOperations.Session createUserSession() {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, internalCluster().getNodeNames()[0]);
        return sqlOperations.createSession(null, new User("normal", ImmutableSet.of(), ImmutableSet.of()), Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal));
        if (nodeOrdinal == 1) {
            builder.put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), false);
        }
        return builder.build();
    }

    @Before
    public void setUpUsersAndPrivileges() throws Exception {
        if (userManager == null) {
            userManager = internalCluster().getInstance(UserManager.class, internalCluster().getNodeNames()[0]);
        }
        for (String userName : USERNAMES) {
            userManager.createUser(userName).get(5, TimeUnit.SECONDS);
        }
        userManager.applyPrivileges(USERNAMES, PRIVILEGES).get(5, TimeUnit.SECONDS);
    }

    @After
    public void dropUsersAndPrivileges() throws Exception {
        for (String userName : USERNAMES) {
            userManager.dropUser(userName, true).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTableColumns() throws Exception {
        execute("select column_name, data_type from information_schema.columns" +
                " where table_name='privileges' and table_schema='sys'", null, createSuperUserSession());
        assertThat(TestingHelpers.printedTable(response.rows()), is("class| string\n" +
                                                                    "grantee| string\n" +
                                                                    "grantor| string\n" +
                                                                    "ident| string\n" +
                                                                    "state| string\n" +
                                                                    "type| string\n"));
    }

    @Test
    public void testListingAsSuperUser() throws Exception {
        execute("select * from sys.privileges order by grantee, type", null, createSuperUserSession());
        assertThat(TestingHelpers.printedTable(response.rows()), is("CLUSTER| Arthur| crate| NULL| GRANT| DML\n" +
                                                                    "CLUSTER| Arthur| crate| NULL| GRANT| DQL\n" +
                                                                    "CLUSTER| Ford| crate| NULL| GRANT| DML\n" +
                                                                    "CLUSTER| Ford| crate| NULL| GRANT| DQL\n"));
    }

    @Test
    public void testListingAsNonSuperUserThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UnauthorizedException: User \"normal\" is not authorized to access table \"sys.privileges\"");
        execute("select * from sys.privileges", null, createUserSession());
    }

    @Test
    public void testTableNotAvailableIfEnterpriseIsOff() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("TableUnknownException: Table 'sys.privileges' unknown");
        execute("select * from sys.privileges", null, createSuperUserSession(false));
    }
}
