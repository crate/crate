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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.common.inject.Singleton;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.metadata.settings.session.SessionSettingProvider;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.types.DataTypes;

@Singleton
public class LoadedRules implements SessionSettingProvider {

    public static final List<Class<? extends Rule<?>>> RULES = buildRules();

    private static List<Class<? extends Rule<?>>> buildRules() {
        var rules = new ArrayList<Rule<?>>();
        rules.addAll(LogicalPlanner.ITERATIVE_OPTIMIZER_RULES);
        rules.addAll(LogicalPlanner.JOIN_ORDER_OPTIMIZER_RULES);
        rules.addAll(LogicalPlanner.FETCH_OPTIMIZER_RULES);
        var result = new ArrayList<Class<? extends Rule<?>>>();
        for (Rule<?> rule : rules) {
            if (rule.mandatory() == false) {
                result.add((Class<? extends Rule<?>>) rule.getClass());
            }
        }
        return result;
    }

    @VisibleForTesting
    static SessionSetting<?> buildRuleSessionSetting(Class<? extends Rule<?>> rule) {
        var optimizerRuleName = Rule.sessionSettingName(rule);
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
            String.format(Locale.ENGLISH, "Indicates if the optimizer rule %s is activated.", rule.getSimpleName()),
            DataTypes.BOOLEAN
        );
    }

    @Override
    public List<SessionSetting<?>> sessionSettings() {
        return Lists2.map(RULES, LoadedRules::buildRuleSessionSetting);
    }
}
