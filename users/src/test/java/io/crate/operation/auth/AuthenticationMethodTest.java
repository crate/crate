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

package io.crate.operation.auth;

import com.google.common.collect.ImmutableList;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManager;
import io.crate.operation.user.UserManagerService;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.service.ClusterService;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.SucceededChannelFuture;
import org.junit.Test;

import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthenticationMethodTest extends CrateUnitTest {

    UserManager fakeUserManager = new UserManagerService(null, null, mock(ClusterService.class)) {

        List<User> users = ImmutableList.of(new User("crate", EnumSet.of(User.Role.SUPERUSER)));

        @Override
        public Iterable<User> users() {
            return users;
        }
    };

    @Test
    public void testTrustAuthentication() throws Exception {
        TrustAuthentication trustAuth = new TrustAuthentication(fakeUserManager);
        assertThat(trustAuth.name(), is("trust"));

        Channel ch = mock(Channel.class);
        when(ch.write(any())).thenReturn(new SucceededChannelFuture(ch));

        trustAuth.pgAuthenticate(ch, "crate")
            .whenComplete((user, throwable) -> {
                assertThat(user.name(), is("crate"));
                assertNull(throwable);
            });

        trustAuth.pgAuthenticate(ch, "cr8")
            .whenComplete((success, throwable) -> {
                assertNotNull(throwable);
            });

    }
}
