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

import java.util.function.UnaryOperator;

import org.elasticsearch.Version;

import io.crate.common.StringUtils;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public interface Rule<T> {

    Pattern<T> pattern();

    /**
     *
     * @param resolvePlan resolvesPlan will resolve a {@GroupReference} to it's referenced {@LogicalPlan} instance.
     *                    It must be used on a source of {@code plan} if the rule needs to access any properties
     *                    of the source other than the outputs or the relation names. This may materialize
     *                    the sub-tree for the source and could be expensive. If the rule doesn't access
     *                    source properties, don't call it.
     */
    LogicalPlan apply(T plan,
                      Captures captures,
                      PlanStats planStats,
                      TransactionContext txnCtx,
                      NodeContext nodeCtx,
                      UnaryOperator<LogicalPlan> resolvePlan,
                      PlannerContext plannerContext);

    /**
     * @return The version all nodes in the cluster must have to be able to use this optimization.
     */
    default Version requiredVersion() {
        return Version.V_4_0_0;
    }

    /**
     * If a rule is mandatory, the rule will always be included in the optimizer rule set and will
     * not be exposed as session setting to be configurable by the user.
     * This is useful for rules which are mandatory for building a valid query execution plan.
     */
    default boolean mandatory() {
        return false;
    }

    default String sessionSettingName() {
        return sessionSettingName(getClass());
    }

    static String sessionSettingName(Class<?> rule) {
        return "optimizer_" + StringUtils.camelToSnakeCase(rule.getSimpleName());
    }
}
