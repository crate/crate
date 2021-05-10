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

import io.crate.execution.engine.collect.stats.JobsLogService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class UserSessionIntegrationTest extends BaseUsersIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        if (nodeOrdinal == 0) { // Enterprise enabled
            return Settings.builder().put(settings)
                .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true).build();
        }
        // Enterprise disabled
        return Settings.builder().put(settings)
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true).build();
    }

    @Test
    public void testSystemExecutorUsesSuperuserSession() {
        systemExecute("select username from sys.jobs", "sys", getNodeByEnterpriseNode(true));
        assertThat(response.rows()[0][0], is("crate"));
    }

    @Test
    public void testSystemExecutorNullUser() {
        systemExecute("select username from sys.jobs", "sys", getNodeByEnterpriseNode(false));
        assertThat(response.rows()[0][0], is("crate"));
    }

    @Test
    public void test_set_session_user_from_auth_superuser_to_unprivileged_user_round_trip() {
        var session = createSuperUserSession();

        execute("SELECT SESSION_USER", session);
        assertThat(response.rows()[0][0], is("crate"));

        execute("CREATE USER test", session);
        execute("SET SESSION AUTHORIZATION test", session);
        execute("SELECT SESSION_USER", session);
        assertThat(response.rows()[0][0], is("test"));

        execute("RESET SESSION AUTHORIZATION", session);
        execute("SELECT SESSION_USER", session);
        assertThat(response.rows()[0][0], is("crate"));
    }

    private String getNodeByEnterpriseNode(boolean enterpriseEnabled) {
        if (enterpriseEnabled) {
            return internalCluster().getNodeNames()[0];
        }
        return internalCluster().getNodeNames()[1];
    }
}
