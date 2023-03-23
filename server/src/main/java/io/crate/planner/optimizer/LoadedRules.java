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

import io.crate.common.StringUtils;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.metadata.settings.session.SessionSettingProvider;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.types.DataTypes;

@Singleton
public class LoadedRules implements SessionSettingProvider {

    private static final String OPTIMIZER_SETTING_PREFIX = "optimizer_";
    public static final List<SessionSetting<?>> RULE_SETTINGS = buildRuleSessingSettings();

    @Override
    public List<SessionSetting<?>> sessionSettings() {
        return RULE_SETTINGS;
    }

    private static List<SessionSetting<?>> buildRuleSessingSettings() {
        var rules = new ArrayList<Rule<?>>();
        rules.addAll(LogicalPlanner.ITERATIVE_OPTIMIZER_RULES);
        rules.addAll(LogicalPlanner.WRITE_OPTIMIZER_RULES);
        rules.addAll(LogicalPlanner.FETCH_OPTIMIZER_RULES);

        return Lists2.map(rules, x -> buildRuleSessionSetting((Class<? extends Rule<?>>) x.getClass()));
    }

    @VisibleForTesting
    public static SessionSetting<?> buildRuleSessionSetting(Class<? extends Rule<?>> rule) {
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
