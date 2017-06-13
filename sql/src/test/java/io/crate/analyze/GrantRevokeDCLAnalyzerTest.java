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

import io.crate.operation.user.User;
import io.crate.operation.user.UserManager;
import io.crate.operation.user.UserManagerProvider;
import io.crate.sql.tree.Privilege;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrantRevokeDCLAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private UserManagerProvider userManagerProvider;
    private UserManager userManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        userManagerProvider = mock(UserManagerProvider.class);
        userManager = mock(UserManager.class);
        when(userManager.findUser(anyString())).thenAnswer(new Answer<User>() {
            @Override
            public User answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                return new User((String) args[0], Collections.emptySet());
            }
        });
        when(userManagerProvider.get()).thenReturn(userManager);
        e = SQLExecutor.builder(clusterService, userManagerProvider).build();
    }

    @Test
    public void testGrantPrivilegesToUsers() {
        GrantRevokePrivilegeAnalyzedStatement analysis = e.analyze("GRANT DQL, DML TO user1, user2");
        assertThat(analysis.userNames().size(), is(2));
        assertThat(analysis.userNames().get(0), is("user1"));
        assertThat(analysis.userNames().get(1), is("user2"));
        assertThat(analysis.privileges().size(), is(2));
        Iterator<Privilege> privilegeIterator = analysis.privileges().iterator();
        assertThat(privilegeIterator.next(), is(Privilege.DQL));
        assertThat(privilegeIterator.next(), is(Privilege.DML));
    }

    @Test
    public void testRevokePrivilegesFromUsers() {
        GrantRevokePrivilegeAnalyzedStatement analysis = e.analyze("REVOKE DQL, DML FROM user1, user2");
        assertThat(analysis.userNames().size(), is(2));
        assertThat(analysis.userNames().get(0), is("user1"));
        assertThat(analysis.userNames().get(1), is("user2"));
        assertThat(analysis.privileges().size(), is(2));
        Iterator<Privilege> privilegeIterator = analysis.privileges().iterator();
        assertThat(privilegeIterator.next(), is(Privilege.DQL));
        assertThat(privilegeIterator.next(), is(Privilege.DML));
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
}
