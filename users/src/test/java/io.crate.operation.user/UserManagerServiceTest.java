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
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Set;

import static io.crate.operation.user.UserManagerService.CRATE_USER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class UserManagerServiceTest extends CrateUnitTest {

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
}
