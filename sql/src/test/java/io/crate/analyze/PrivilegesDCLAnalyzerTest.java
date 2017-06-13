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

package io.crate.analyze;

import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.user.Privilege.Type;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManager;
import io.crate.operation.user.UserManagerProvider;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static io.crate.analyze.user.Privilege.Type.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.isIn;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class PrivilegesDCLAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static final User GRANTOR_TEST_USER = new User("test", Collections.emptySet(), Collections.emptySet());

    private SQLExecutor e;
    private UserManagerProvider userManagerProvider;
    private UserManager userManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        userManagerProvider = mock(UserManagerProvider.class);
        userManager = mock(UserManager.class);

        when(userManager.findUser(anyString())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            return new User((String) args[0], Collections.emptySet(), Collections.emptySet());
        });
        when(userManagerProvider.get()).thenReturn(userManager);

        e = SQLExecutor.builder(clusterService, userManagerProvider).build();
    }

    @Test
    public void testGrantPrivilegesToUsers() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("GRANT DQL, DML TO user1, user2");
        assertThat(analysis.userNames().size(), is(2));
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges().size(), is(2));
        verifyAnalysisContains(analysis, DQL, DML);
    }

    @Test
    public void testRevokePrivilegesFromUsers() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("REVOKE DQL, DML FROM user1, user2");
        assertThat(analysis.userNames().size(), is(2));
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges().size(), is(2));
        verifyAnalysisContains(analysis, DQL, DML);
    }

    @Test
    public void testGrantRevokeAllPrivileges() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("GRANT ALL PRIVILEGES TO user1");
        assertThat(analysis.privileges().size(), is(3));
        verifyAnalysisContains(analysis, DDL, DML, DQL);

        analysis = analyzePrivilegesStatement("REVOKE ALL PRIVILEGES FROM user1");
        assertThat(analysis.privileges().size(), is(3));
        verifyAnalysisContains(analysis, DDL, DML, DQL);
    }

    private void verifyAnalysisContains(PrivilegesAnalyzedStatement analysis, Type... types) {
        for (io.crate.analyze.user.Privilege privilege : analysis.privileges()) {
            assertThat(privilege.type(), isIn(types));
        }
    }

    private PrivilegesAnalyzedStatement analyzePrivilegesStatement(String statement) {
        return (PrivilegesAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement(statement),
            new SessionContext(0, Option.NONE, null, GRANTOR_TEST_USER), null
        ).analyzedStatement();
    }

    @Test
    public void testGrantToUnknownUserThrowsException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("User test does not exists");
        e = SQLExecutor.builder(clusterService).build();
        e.analyze("GRANT DQL TO test");
    }

    @Test
    public void testRevokeFromUnknownUserThrowsException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("User test does not exists");
        e = SQLExecutor.builder(clusterService).build();
        e.analyze("REVOKE DQL FROM test");
    }

    @Test
    public void testGrantPrivilegeToSuperUserThrowsException() {
        reset(userManagerProvider);
        reset(userManager);
        when(userManager.findUser(anyString())).thenReturn(
            new User("test_superuser", Collections.singleton(User.Role.SUPERUSER), Collections.emptySet())
        );
        when(userManagerProvider.get()).thenReturn(userManager);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot alter privileges for superuser test");
        e = SQLExecutor.builder(clusterService, userManagerProvider).build();
        e.analyze("GRANT DQL TO test");
    }
}
