/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.collect.Sets;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.ResetStatement;
import io.crate.sql.tree.SetStatement;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.Locale;
import java.util.Set;

public class SetStatementAnalyzer extends AbstractStatementAnalyzer<Void, SetAnalysis> {

    private final ESLogger logger = Loggers.getLogger(this.getClass());

    @Override
    public Void visitSetStatement(SetStatement node, SetAnalysis context) {
        context.persistent(node.settingType().equals(SetStatement.SettingType.PERSISTENT));
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        for (Assignment assignment : node.assignments()) {
            String settingsName = normalizeKey(
                    ExpressionToStringVisitor.convert(assignment.columnName(), context.parameters())
            );
            SettingsApplier settingsApplier = context.getSetting(settingsName);
            if (settingsApplier == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "setting '%s' not supported", settingsName));
            }
            settingsApplier.apply(builder, context.parameters(), assignment.expression());
        }
        context.settings(builder.build());
        return null;
    }

    @Override
    public Void visitResetStatement(ResetStatement node, SetAnalysis context) {
        context.isReset(true);
        Set<String> settingsToRemove = Sets.newHashSet();
        for (Expression expression : node.columns()) {
            String settingsName = normalizeKey(
                    ExpressionToStringVisitor.convert(expression, context.parameters())
            );
            if (!settingsToRemove.contains(settingsName)) {
                Set<String> settingNames = context.settingNamesByPrefix(settingsName);
                if (settingNames.size() == 0) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "setting '%s' not supported", settingsName));
                }
                settingsToRemove.addAll(settingNames);
            }
        }
        context.settingsToRemove(settingsToRemove);
        return null;
    }

    public String normalizeKey(String key) {
        if (!key.startsWith("cluster.")) {
            return String.format("cluster.%s", key);
        }
        return key;
    }
}
