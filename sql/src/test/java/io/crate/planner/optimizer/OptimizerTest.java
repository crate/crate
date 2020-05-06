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

package io.crate.planner.optimizer;

import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.SessionSettings;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveFilterBeneathHashJoin;
import io.crate.planner.optimizer.rule.RewriteCollectToGet;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


public class OptimizerTest {

    @Test
    public void test_rule_filtering() {
        SessionSettings sessionSettings = new SessionSettings("User",
                                                              SearchPath.pathWithPGCatalogAndDoc(),
                                                              true,
                                                              Set.of("MergeFilters"));
        List<Rule<?>> rules = Optimizer.filterRulesFromContext(List.of(new MergeFilters()),
                                                               TransactionContext.of(sessionSettings));
        assertThat(rules.isEmpty(), is(true));

        rules = Optimizer.filterRulesFromContext(List.of(new MoveFilterBeneathHashJoin()), TransactionContext.of(sessionSettings));

        assertThat(rules.size(), is(1));
    }
}
