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

package io.crate.replication.logical.plan;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.crate.exceptions.InvalidArgumentException;
import io.crate.planner.PlannerContext;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CreateSubscriptionPlanTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_create_subscription_plan_checks_subscribing_user() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        Assertions.assertThatThrownBy(() -> {
            CreateSubscriptionPlan plan = e.plan("CREATE SUBSCRIPTION sub CONNECTION 'crate://example.com' publication pub1");
            PlannerContext plannerContext = e.getPlannerContext(clusterService.state());
            plan.executeOrFail(null, plannerContext, null, null, null);
        })
            .isExactlyInstanceOf(InvalidArgumentException.class)
            .hasMessageContaining("Setting 'user' must be provided on CREATE SUBSCRIPTION");
    }

}
