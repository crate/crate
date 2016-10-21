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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.core.collections.Row;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.Setting;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.sql.tree.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

public class SetStatementAnalyzer {

    private static final ESLogger logger = Loggers.getLogger(SetStatementAnalyzer.class);

    private SetStatementAnalyzer() {
    }

    public static SetAnalyzedStatement analyze(SetStatement node, Analysis context) {
        boolean isPersistent = node.settingType().equals(SetStatement.SettingType.PERSISTENT);

        if (!SetStatement.Scope.GLOBAL.equals(node.scope())) {
            Assignment assignment = node.assignments().get(0);
            String setting = ExpressionToStringVisitor.convert(assignment.columnName(), Row.EMPTY);
            Set<String> settingParts = CrateSettings.settingNamesByPrefix(setting);
            if (settingParts.size() != 0) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "GLOBAL Cluster setting '%s' cannot be used with SET SESSION / LOCAL ", setting));
            }
            Settings.Builder builder = Settings.builder()
                .put(setting, assignment.expressions());
            return new SetAnalyzedStatement(node.scope(), builder.build(), isPersistent);
        }

        Settings.Builder builder = Settings.builder();
        for (Assignment assignment : node.assignments()) {
            String settingsName = ExpressionToStringVisitor.convert(assignment.columnName(), Row.EMPTY);

            SettingsApplier settingsApplier = CrateSettings.getSettingsApplier(settingsName);
            for (String setting : ExpressionToSettingNameListVisitor.convert(assignment)) {
                checkIfSettingIsRuntime(setting);
            }
            settingsApplier.apply(builder, context.parameterContext().parameters(), assignment.expression());
        }
        return new SetAnalyzedStatement(node.scope(), builder.build(), isPersistent);
    }

    public static ResetAnalyzedStatement analyze(ResetStatement node) {
        Set<String> settingsToRemove = Sets.newHashSet();
        for (Expression expression : node.columns()) {
            String settingsName = ExpressionToStringVisitor.convert(expression, Row.EMPTY);
            if (!settingsToRemove.contains(settingsName)) {
                Set<String> settingNames = CrateSettings.settingNamesByPrefix(settingsName);
                if (settingNames.size() == 0) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "setting '%s' not supported", settingsName));
                }
                for (String setting : settingNames) {
                    checkIfSettingIsRuntime(setting);
                }
                settingsToRemove.addAll(settingNames);
                logger.info("resetting [{}]", settingNames);
            }
        }
        return new ResetAnalyzedStatement(settingsToRemove);
    }

    private static void checkIfSettingIsRuntime(String name) {
        checkIfSettingIsRuntime(CrateSettings.SETTINGS, name);
    }

    private static void checkIfSettingIsRuntime(List<Setting> settings, String name) {
        for (Setting<?, ?> setting : settings) {
            if (setting.settingName().equals(name) && !setting.isRuntime()) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "setting '%s' cannot be set/reset at runtime", name));
            }
            checkIfSettingIsRuntime(setting.children(), name);
        }
    }

    private static class ExpressionToSettingNameListVisitor extends AstVisitor<Collection<String>, String> {

        private static final ExpressionToSettingNameListVisitor INSTANCE = new ExpressionToSettingNameListVisitor();

        private ExpressionToSettingNameListVisitor() {
        }

        public static Collection<String> convert(Node node) {
            return INSTANCE.process(node, null);
        }

        @Override
        public Collection<String> visitAssignment(Assignment node, String context) {
            String left = ExpressionToStringVisitor.convert(node.columnName(), Row.EMPTY);
            return node.expression().accept(this, left);
        }

        @Override
        public Collection<String> visitObjectLiteral(ObjectLiteral node, String context) {
            Collection<String> settingNames = new ArrayList<>();
            for (Map.Entry<String, Expression> entry : node.values().entries()) {
                String s = String.format(Locale.ENGLISH, "%s.%s", context, entry.getKey());
                if (entry.getValue() instanceof ObjectLiteral) {
                    settingNames.addAll(entry.getValue().accept(this, s));
                } else {
                    settingNames.add(s);
                }
            }
            return settingNames;
        }

        @Override
        protected Collection<String> visitLiteral(Literal node, String context) {
            return ImmutableList.of(context);
        }

        @Override
        public Collection<String> visitParameterExpression(ParameterExpression node, String context) {
            return ImmutableList.of(context);
        }

        @Override
        protected Collection<String> visitNode(Node node, String context) {
            return ImmutableList.of(context);
        }
    }
}
