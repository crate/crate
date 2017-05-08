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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import io.crate.exceptions.UnauthorizedException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.junit.Test;

import java.util.Set;

import static io.crate.operation.user.UserManagerService.CRATE_USER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class UserManagerServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testNullAndEmptyMetaData() {
        // the users list will always contain a crate user
        Set<User> users = UserManagerService.getUsers(null);
        assertThat(users, contains(CRATE_USER));

        users = UserManagerService.getUsers(new UsersMetaData());
        assertThat(users, contains(CRATE_USER));
    }

    @Test
    public void testNewUser() {
        Set<User> users = UserManagerService.getUsers(new UsersMetaData(ImmutableList.of("arthur")));
        assertThat(users, containsInAnyOrder(new User("arthur", ImmutableSet.of()), CRATE_USER));
    }

    @Test
    public void testCreateUserStatementCheckPermissionFalse() {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("User \"null\" is not authorized to execute statement");
        UserManagerService userManagerService = new UserManagerService(null, null, clusterService);
        userManagerService.ensureAuthorized(new CreateUserAnalyzedStatement(""),
            new SessionContext(0, Option.NONE, "my_schema", null));
    }

    @Test
    public void testCreateUserStatementCheckPermissionTrue() {
        UserManagerService userManagerService = new UserManagerService(null, null, clusterService);
        userManagerService.ensureAuthorized(new CreateUserAnalyzedStatement("bla"),
            new SessionContext(0, Option.NONE, "my_schema", UserManagerService.CRATE_USER));
    }

    @Test
    public void testDropUserStatementCheckPermissionFalse() {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("User \"null\" is not authorized to execute statement");
        UserManagerService userManagerService = new UserManagerService(null, null, clusterService);
        userManagerService.ensureAuthorized(new DropUserAnalyzedStatement("", false),
            new SessionContext(0, Option.NONE, "my_schema", null));
    }

    @Test
    public void testDropUserStatementCheckPermissionTrue() {
        UserManagerService userManagerService = new UserManagerService(null, null, clusterService);
        userManagerService.ensureAuthorized(new DropUserAnalyzedStatement("bla", false),
            new SessionContext(0, Option.NONE, "my_schema", UserManagerService.CRATE_USER));
    }
}
