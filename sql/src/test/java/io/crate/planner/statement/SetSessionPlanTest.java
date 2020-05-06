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

package io.crate.planner.statement;

import io.crate.action.sql.SessionContext;
import io.crate.auth.user.User;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;


import java.util.Set;

import static io.crate.planner.statement.SetSessionPlan.validateSetting;
import static io.crate.planner.statement.SetSessionPlan.addOptimizerRuleSetting;
import static org.hamcrest.Matchers.hasItems;

public class SetSessionPlanTest extends CrateUnitTest {

    @Test
    public void testSetSessionInvalidSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "GLOBAL Cluster setting 'stats.operations_log_size' cannot be used with SET SESSION / LOCAL");
        validateSetting("stats.operations_log_size");
    }

    @Test
    public void test_optimizer_rule_settings() {
        SessionContext sessionContext = new SessionContext(Set.of(), User.of("user"), "test");
        addOptimizerRuleSetting(sessionContext, "test_rule", false);
        assertThat(sessionContext.excludedOptimizerRules(), hasItems("TestRule"));
    }
}
