/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Random;

import org.junit.Test;

import io.crate.common.collections.Lists2;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.planner.optimizer.LoadedRules;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.MoveFilterBeneathGroupBy;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.rule.MoveLimitBeneathEval;
import io.crate.planner.optimizer.rule.MoveOrderBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.RewriteFilterOnOuterJoinToInnerJoin;
import io.crate.planner.optimizer.rule.RewriteNestedLoopJoinToHashJoin;
import io.crate.planner.optimizer.rule.RewriteToQueryThenFetch;

public class SqlTransportExecutorTest {

    @Test
    public void test_rule_randomization() {
        List<Class<? extends Rule<?>>> allRules = List.of(
            DeduplicateOrder.class,
            MoveFilterBeneathOrder.class,
            RewriteToQueryThenFetch.class,
            MoveLimitBeneathEval.class,
            MergeFilterAndCollect.class,
            RewriteToQueryThenFetch.class,
            RewriteNestedLoopJoinToHashJoin.class,
            RewriteFilterOnOuterJoinToInnerJoin.class,
            MoveOrderBeneathFetchOrEval.class,
            MoveFilterBeneathGroupBy.class
        );

        List<SessionSetting<?>> rulesAsSessionSettings = Lists2.map(allRules, LoadedRules::buildRuleSessionSetting);

        var result = SQLTransportExecutor.buildRandomizedRuleSessionSettings(new Random(1L),
                                                                             0.0,
                                                                             rulesAsSessionSettings,
                                                                             List.of());
        assertThat(result).hasSize(0);

        result = SQLTransportExecutor.buildRandomizedRuleSessionSettings(new Random(1L),
                                                                         0.3,
                                                                         rulesAsSessionSettings,
                                                                         List.of());
        assertThat(result).hasSize(3);
        assertThat(result).containsExactly("set optimizer_rewrite_nested_loop_join_to_hash_join=false",
                                           "set optimizer_move_filter_beneath_group_by=false",
                                           "set optimizer_rewrite_filter_on_outer_join_to_inner_join=false");

        result = SQLTransportExecutor.buildRandomizedRuleSessionSettings(new Random(1L),
                                                                         1.0,
                                                                         rulesAsSessionSettings,
                                                                         List.of());
        assertThat(result).hasSize(10);
        assertThat(result).containsExactly("set optimizer_rewrite_nested_loop_join_to_hash_join=false",
                                           "set optimizer_move_filter_beneath_group_by=false",
                                           "set optimizer_rewrite_filter_on_outer_join_to_inner_join=false",
                                           "set optimizer_move_order_beneath_fetch_or_eval=false",
                                           "set optimizer_merge_filter_and_collect=false",
                                           "set optimizer_rewrite_to_query_then_fetch=false",
                                           "set optimizer_deduplicate_order=false",
                                           "set optimizer_move_limit_beneath_eval=false",
                                           "set optimizer_move_filter_beneath_order=false",
                                           "set optimizer_rewrite_to_query_then_fetch=false");
    }

    @Test
    public void test_rule_randomization_with_rule_kept() {
        List<Class<? extends Rule<?>>> allRules = List.of(
            DeduplicateOrder.class,
            MoveFilterBeneathOrder.class,
            RewriteToQueryThenFetch.class,
            MoveLimitBeneathEval.class
        );

        List<SessionSetting<?>> rulesAsSessionSettings = Lists2.map(allRules, LoadedRules::buildRuleSessionSetting);
        List<Class<? extends Rule<?>>> ensureRule = List.of(DeduplicateOrder.class);

        var result = SQLTransportExecutor.buildRandomizedRuleSessionSettings(new Random(1L),
                                                                             1,
                                                                             rulesAsSessionSettings,
                                                                             ensureRule);

        assertThat(result).hasSize(3);
        assertThat(result).containsExactly(
            "set optimizer_rewrite_to_query_then_fetch=false",
            "set optimizer_move_limit_beneath_eval=false",
            "set optimizer_move_filter_beneath_order=false");
    }

    @Test
    public void test_rule_randomization_with__two_rules_kept() {
        List<Class<? extends Rule<?>>> allRules = List.of(
            DeduplicateOrder.class,
            MoveFilterBeneathOrder.class,
            RewriteToQueryThenFetch.class,
            MoveLimitBeneathEval.class
        );

        List<SessionSetting<?>> rulesAsSessionSettings = Lists2.map(allRules, LoadedRules::buildRuleSessionSetting);
        List<Class<? extends Rule<?>>> ensureRule = List.of(DeduplicateOrder.class, MoveFilterBeneathOrder.class);

        var result = SQLTransportExecutor.buildRandomizedRuleSessionSettings(new Random(1L),
                                                                             1,
                                                                             rulesAsSessionSettings,
                                                                             ensureRule);

        assertThat(result).hasSize(2);
        assertThat(result).containsExactly("set optimizer_rewrite_to_query_then_fetch=false",
                                           "set optimizer_move_limit_beneath_eval=false");
    }
}
