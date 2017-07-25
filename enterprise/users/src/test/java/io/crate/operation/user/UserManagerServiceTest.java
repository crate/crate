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
import io.crate.metadata.UsersMetaData;
import io.crate.metadata.UsersPrivilegesMetaData;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.operation.collect.sources.SysTableRegistry;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static io.crate.operation.user.UserManagerService.*;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class UserManagerServiceTest extends CrateDummyClusterServiceUnitTest {

    private UserManagerService userManagerService;

    @Before
    public void setUpUserManager() throws Exception {
        userManagerService = new UserManagerService(null, null,
            null, null,
            mock(SysTableRegistry.class), clusterService, new DDLClusterStateService());
    }

    @Test
    public void testNullAndEmptyMetaData() {
        // the users list will always contain a crate user
        Set<User> users = UserManagerService.getUsers(null, null);
        assertThat(users, contains(CRATE_USER));

        users = UserManagerService.getUsers(new UsersMetaData(), new UsersPrivilegesMetaData());
        assertThat(users, contains(CRATE_USER));
    }

    @Test
    public void testNewUser() {
        Set<User> users = UserManagerService.getUsers(new UsersMetaData(ImmutableList.of("arthur")), new UsersPrivilegesMetaData());
        assertThat(users, containsInAnyOrder(new User("arthur", ImmutableSet.of(), ImmutableSet.of()), CRATE_USER));
    }

    @Test
    public void testGetNoopStatementValidatorForNullUser() throws Exception {
        StatementAuthorizedValidator validator = userManagerService.getStatementValidator(null);
        assertThat(validator, is(ALWAYS_FAIL_STATEMENT_VALIDATOR));
    }

    @Test
    public void testGetNoopStatementValidatorForSuperUser() throws Exception {
        StatementAuthorizedValidator validator = userManagerService.getStatementValidator(CRATE_USER);
        assertThat(validator, is(NOOP_STATEMENT_VALIDATOR));
    }

    @Test
    public void testGetNoopExceptionValidatorForNullUser() throws Exception {
        ExceptionAuthorizedValidator validator = userManagerService.getExceptionValidator(null);
        assertThat(validator, is(ALWAYS_FAIL_EXCEPTION_VALIDATOR));
    }

    @Test
    public void testGetNoopExceptionValidatorForSuperUser() throws Exception {
        ExceptionAuthorizedValidator validator = userManagerService.getExceptionValidator(CRATE_USER);
        assertThat(validator, is(NOOP_EXCEPTION_VALIDATOR));
    }
}
