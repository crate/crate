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

package io.crate.operation.user;

import io.crate.analyze.user.Privilege;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.util.set.Sets;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class UserTest extends CrateUnitTest {

    private static final User USER_WITH_DQL = new User("test", Collections.emptySet(), Sets.newHashSet(
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")
    ));

    @Test
    public void testUserDoesNotHasPrivilege() throws Exception {
        User user = new User("test", Collections.emptySet(), Collections.emptySet());
        assertThat(user.hasPrivilege(Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null), is(false));
        assertThat(user.hasAnyPrivilege(Privilege.Clazz.CLUSTER, null), is(false));
    }

    @Test
    public void testUserDoesNotHasRequiredPrivilege() throws Exception {
        assertThat(USER_WITH_DQL.hasPrivilege(Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null), is(false));
        assertThat(USER_WITH_DQL.hasAnyPrivilege(Privilege.Clazz.CLUSTER, null), is(true));
    }

    @Test
    public void testUserHasRequiredPrivilege() throws Exception {
        assertThat(USER_WITH_DQL.hasPrivilege(Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null), is(true));
    }
}
