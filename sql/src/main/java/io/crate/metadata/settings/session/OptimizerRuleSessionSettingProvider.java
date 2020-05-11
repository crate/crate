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

package io.crate.metadata.settings.session;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import io.crate.common.collections.Lists2;
import io.crate.planner.optimizer.Rule;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Locale;

public class OptimizerRuleSessionSettingProvider implements SessionSettingProvider {

    private static final String OPTIMIZER_RULE = "optimizer_";

    @Override
    public List<SessionSetting<?>> sessionSettings() {
       return Lists2.map(Rule.IMPLEMENTATIONS, OptimizerRuleSessionSettingProvider::buildRuleSessionSetting);
    }

    @VisibleForTesting
    static SessionSetting<?> buildRuleSessionSetting(Class<?> rule) {
        var fullName = rule.getName();
        var simpleName = rule.getSimpleName();
        var optimizerRuleName = OPTIMIZER_RULE + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, simpleName);
        return new SessionSetting<>(
            optimizerRuleName,
            objects -> {
            },
            objects -> DataTypes.BOOLEAN.value(objects[0]),
            (sessionContext, activateRule) -> {
                if (activateRule) {
                    // All rules are activated by default
                    sessionContext.excludedOptimizerRules().remove(fullName);
                } else {
                    sessionContext.excludedOptimizerRules().add(fullName);
                }
            },
            s -> String.valueOf(s.excludedOptimizerRules().contains(fullName) == false),
            () -> String.valueOf(true),
            String.format(Locale.ENGLISH, "Indicates if the optimizer rule %s is activated.", simpleName),
            DataTypes.BOOLEAN.getName()
        );
    }
}
