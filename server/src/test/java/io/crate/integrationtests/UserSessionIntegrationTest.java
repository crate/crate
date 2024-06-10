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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class UserSessionIntegrationTest extends BaseRolesIntegrationTest {

    @Test
    public void test_set_session_user_from_auth_superuser_to_unprivileged_user_round_trip() {
        try (var session = sqlExecutor.newSession()) {
            execute("SELECT SESSION_USER", session);
            assertThat(response).hasRows("crate");

            execute("CREATE USER test", session);
            execute("SET SESSION AUTHORIZATION test", session);
            execute("SELECT SESSION_USER", session);
            assertThat(response).hasRows("test");

            execute("RESET SESSION AUTHORIZATION", session);
            execute("SELECT SESSION_USER", session);
            assertThat(response).hasRows("crate");
        }
    }
}
