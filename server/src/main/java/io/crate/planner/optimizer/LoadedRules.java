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

import java.util.List;
import java.util.Locale;

import org.elasticsearch.common.inject.Singleton;

import io.crate.common.StringUtils;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.metadata.settings.session.SessionSettingProvider;
import io.crate.planner.operators.RewriteInsertFromSubQueryToInsertFromValues;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.MergeAggregateAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeAggregateRenameAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveFilterBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveFilterBeneathGroupBy;
import io.crate.planner.optimizer.rule.MoveFilterBeneathHashJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.rule.MoveFilterBeneathProjectSet;
import io.crate.planner.optimizer.rule.MoveFilterBeneathRename;
import io.crate.planner.optimizer.rule.MoveFilterBeneathUnion;
import io.crate.planner.optimizer.rule.MoveFilterBeneathWindowAgg;
import io.crate.planner.optimizer.rule.MoveOrderBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveOrderBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveOrderBeneathRename;
import io.crate.planner.optimizer.rule.MoveOrderBeneathUnion;
import io.crate.planner.optimizer.rule.OptimizeCollectWhereClauseAccess;
import io.crate.planner.optimizer.rule.RemoveRedundantFetchOrEval;
import io.crate.planner.optimizer.rule.RewriteFilterOnOuterJoinToInnerJoin;
import io.crate.planner.optimizer.rule.RewriteGroupByKeysLimitToLimitDistinct;
import io.crate.planner.optimizer.rule.RewriteToQueryThenFetch;
import io.crate.types.DataTypes;

@Singleton
public class LoadedRules implements SessionSettingProvider {

    private static final String OPTIMIZER_SETTING_PREFIX = "optimizer_";

    private final List<Class<? extends Rule<?>>> rules = List.of(
        RemoveRedundantFetchOrEval.class,
        MergeAggregateAndCollectToCount.class,
        MergeAggregateRenameAndCollectToCount.class,
        MergeFilters.class,
        MoveFilterBeneathRename.class,
        MoveFilterBeneathFetchOrEval.class,
        MoveFilterBeneathOrder.class,
        MoveFilterBeneathProjectSet.class,
        MoveFilterBeneathHashJoin.class,
        MoveFilterBeneathNestedLoop.class,
        MoveFilterBeneathUnion.class,
        MoveFilterBeneathGroupBy.class,
        MoveFilterBeneathWindowAgg.class,
        MergeFilterAndCollect.class,
        RewriteFilterOnOuterJoinToInnerJoin.class,
        MoveOrderBeneathUnion.class,
        MoveOrderBeneathNestedLoop.class,
        MoveOrderBeneathFetchOrEval.class,
        MoveOrderBeneathRename.class,
        DeduplicateOrder.class,
        OptimizeCollectWhereClauseAccess.class,
        RewriteGroupByKeysLimitToLimitDistinct.class,
        RewriteInsertFromSubQueryToInsertFromValues.class,
        RewriteToQueryThenFetch.class
    );

    @Override
    public List<SessionSetting<?>> sessionSettings() {
        return Lists2.map(rules, this::buildRuleSessionSetting);
    }

    @VisibleForTesting
    SessionSetting<?> buildRuleSessionSetting(Class<? extends Rule<?>> rule) {
        var simpleName = rule.getSimpleName();
        var optimizerRuleName = OPTIMIZER_SETTING_PREFIX + StringUtils.camelToSnakeCase(simpleName);
        return new SessionSetting<>(
            optimizerRuleName,
            objects -> {},
            objects -> DataTypes.BOOLEAN.sanitizeValue(objects[0]),
            (sessionSettings, activateRule) -> {
                if (activateRule) {
                    // All rules are activated by default
                    sessionSettings.excludedOptimizerRules().remove(rule);
                } else {
                    sessionSettings.excludedOptimizerRules().add(rule);
                }
            },
            s -> {
                if (s instanceof CoordinatorSessionSettings cs) {
                    return String.valueOf(cs.excludedOptimizerRules().contains(rule) == false);
                }
                return "false";
            },
            () -> String.valueOf(true),
            String.format(Locale.ENGLISH, "Indicates if the optimizer rule %s is activated.", simpleName),
            DataTypes.BOOLEAN
        );
    }
}
