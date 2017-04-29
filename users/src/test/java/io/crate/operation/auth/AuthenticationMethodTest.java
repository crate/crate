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

import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.SucceededChannelFuture;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthenticationMethodTest extends CrateUnitTest {

    @Test
    public void testTrustAuthentication() throws Exception {
        TrustAuthentication trustAuth = new TrustAuthentication();
        assertThat(trustAuth.name(), is("trust"));

        Channel ch = mock(Channel.class);
        when(ch.write(any())).thenReturn(new SucceededChannelFuture(ch));

        SessionContext session = new SessionContext(0, Option.NONE, null, "crate");
        trustAuth.pgAuthenticate(ch, session)
            .whenComplete((success, throwable) -> {
                assertTrue(success);
                assertNull(throwable);
            });

        session = new SessionContext(0, Option.NONE, null, "cr8");
        trustAuth.pgAuthenticate(ch, session)
            .whenComplete((success, throwable) -> {
                assertFalse(success);
                assertNull(throwable);
            });

    }
}
