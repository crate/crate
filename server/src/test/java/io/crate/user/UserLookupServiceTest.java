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

package io.crate.user;

import static io.crate.user.User.CRATE_USER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.user.metadata.UserDefinitions;
import io.crate.user.metadata.UsersMetadata;
import io.crate.user.metadata.UsersPrivilegesMetadata;

public class UserLookupServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testNullAndEmptyMetadata() {
        // the users list will always contain a crate user
        Set<User> users = UserLookupService.getUsers(null, null);
        assertThat(users).containsExactly(CRATE_USER);

        users = UserLookupService.getUsers(new UsersMetadata(), new UsersPrivilegesMetadata());
        assertThat(users).containsExactly(CRATE_USER);
    }

    @Test
    public void testNewUser() {
        Set<User> users = UserLookupService.getUsers(new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY), new UsersPrivilegesMetadata());
        assertThat(users).containsExactlyInAnyOrder(User.of("Arthur"), CRATE_USER);
    }
}
