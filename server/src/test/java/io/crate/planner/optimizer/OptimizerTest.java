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

package io.crate.planner.optimizer;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveFilterBeneathJoin;
import io.crate.planner.optimizer.rule.RewriteJoinPlan;
import io.crate.user.Role;

public class OptimizerTest {

    @Test
    public void test_rule_filtering() {
        var sessionSettings = new CoordinatorSessionSettings(
            Role.userOf("User"),
            Role.userOf("User"),
            SearchPath.pathWithPGCatalogAndDoc(),
            true,
            Set.of(MergeFilters.class),
            true,
            0
        );

        List<Rule<?>> rules = Optimizer.removeExcludedRules(List.of(new MergeFilters()), sessionSettings.excludedOptimizerRules());
        assertThat(rules).isEmpty();
        var moveFilterBeneathJoin = new MoveFilterBeneathJoin();
        rules = Optimizer.removeExcludedRules(List.of(moveFilterBeneathJoin), sessionSettings.excludedOptimizerRules());
        assertThat(rules).contains(moveFilterBeneathJoin);
    }

    @Test
    public void test_non_removable_rules_are_not_filtered() {
        var sessionSettings = new CoordinatorSessionSettings(
            Role.userOf("User"),
            Role.userOf("User"),
            SearchPath.pathWithPGCatalogAndDoc(),
            true,
            Set.of(RewriteJoinPlan.class), // this rule can not be removed
            true,
            0
        );
        var rewriteJoinPlan = new RewriteJoinPlan();
        List<Rule<?>> rules = Optimizer.removeExcludedRules(List.of(rewriteJoinPlan), sessionSettings.excludedOptimizerRules());
        assertThat(rules).contains(rewriteJoinPlan);
    }
}
