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

import static io.crate.testing.SQLTransportExecutor.buildRandomizedRuleSessionSettings;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Random;

import org.junit.Test;

import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.MoveFilterBeneathGroupBy;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.rule.MoveLimitBeneathEval;
import io.crate.planner.optimizer.rule.MoveOrderBeneathEval;
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
            MoveOrderBeneathEval.class,
            MoveFilterBeneathGroupBy.class
        );

        assertThatThrownBy(() -> buildRandomizedRuleSessionSettings(new Random(1L), 0.0, allRules, List.of()))
            .isInstanceOf(AssertionError.class).hasMessage("Percentage of rules to disable for Rule Randomization must greater than 0 and equal or less than 1");

        assertThat(buildRandomizedRuleSessionSettings(new Random(1L), 0.3, allRules, List.of()))
            .hasSize(3)
            .containsExactly(
                "set optimizer_rewrite_nested_loop_join_to_hash_join=false",
                "set optimizer_move_filter_beneath_group_by=false",
                "set optimizer_rewrite_filter_on_outer_join_to_inner_join=false"
            );

        assertThat(buildRandomizedRuleSessionSettings(new Random(1L), 1.0, allRules, List.of()))
            .hasSize(10)
            .containsExactly(
                "set optimizer_rewrite_nested_loop_join_to_hash_join=false",
                "set optimizer_move_filter_beneath_group_by=false",
                "set optimizer_rewrite_filter_on_outer_join_to_inner_join=false",
                "set optimizer_move_order_beneath_eval=false",
                "set optimizer_merge_filter_and_collect=false",
                "set optimizer_rewrite_to_query_then_fetch=false",
                "set optimizer_deduplicate_order=false",
                "set optimizer_move_limit_beneath_eval=false",
                "set optimizer_move_filter_beneath_order=false",
                "set optimizer_rewrite_to_query_then_fetch=false"
            );

        assertThatThrownBy(() -> buildRandomizedRuleSessionSettings(new Random(1L), 1.1, allRules, List.of()))
            .isInstanceOf(AssertionError.class).hasMessage("Percentage of rules to disable for Rule Randomization must greater than 0 and equal or less than 1");
    }

    @Test
    public void test_rule_randomization_with_rule_to_keep() {
        List<Class<? extends Rule<?>>> allRules = List.of(
            DeduplicateOrder.class,
            MoveFilterBeneathOrder.class,
            RewriteToQueryThenFetch.class,
            MoveLimitBeneathEval.class
        );

        List<Class<? extends Rule<?>>> rulesToKeep = List.of(DeduplicateOrder.class);

        assertThat(buildRandomizedRuleSessionSettings(new Random(1L), 1.0, allRules, rulesToKeep))
            .hasSize(3)
            .containsExactly(
                "set optimizer_rewrite_to_query_then_fetch=false",
                "set optimizer_move_limit_beneath_eval=false",
                "set optimizer_move_filter_beneath_order=false"
            );
    }

    @Test
    public void test_rule_randomization_with_rules_to_keep() {
        List<Class<? extends Rule<?>>> allRules = List.of(
            DeduplicateOrder.class,
            MoveFilterBeneathOrder.class,
            RewriteToQueryThenFetch.class,
            MoveLimitBeneathEval.class
        );

        List<Class<? extends Rule<?>>> rulesToKeep = List.of(DeduplicateOrder.class, MoveFilterBeneathOrder.class);

        assertThat(buildRandomizedRuleSessionSettings(new Random(1L), 1.0, allRules, rulesToKeep))
            .hasSize(2)
            .containsExactly(
                "set optimizer_rewrite_to_query_then_fetch=false",
                "set optimizer_move_limit_beneath_eval=false"
            );
    }

    @Test
    public void test_rule_randomization_with_one_rule() {
        List<Class<? extends Rule<?>>> allRules = List.of(DeduplicateOrder.class);

        assertThat(buildRandomizedRuleSessionSettings(new Random(1L), 0.1, allRules, List.of()))
            .hasSize(1)
            .containsExactly("set optimizer_deduplicate_order=false");

        assertThat(buildRandomizedRuleSessionSettings(new Random(1L),
                                                      0.1,
                                                      allRules,
                                                      List.of(DeduplicateOrder.class))).isEmpty();

    }

    @Test
    public void test_rule_randomization_with_two_rules() {
        List<Class<? extends Rule<?>>> allRules = List.of(DeduplicateOrder.class, MoveFilterBeneathOrder.class);

        assertThat(buildRandomizedRuleSessionSettings(new Random(1L), 0.5, allRules, List.of()))
            .hasSize(1)
            .containsExactly("set optimizer_deduplicate_order=false");

    }

}
