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
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.Setting;
import io.crate.sql.tree.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

public class SetStatementAnalyzer {

    private static final ESLogger logger = Loggers.getLogger(SetStatementAnalyzer.class);
    private SetStatementAnalyzer() {}

    public static SetAnalyzedStatement analyze(SetStatement node, ParameterContext parameterContext) {
        Settings.Builder builder = Settings.builder();
        for (Assignment assignment : node.assignments()) {
            String settingsName = ExpressionToStringVisitor.convert(assignment.columnName(),
                    parameterContext.parameters());

            SettingsApplier settingsApplier = CrateSettings.getSetting(settingsName);
            for (String setting : ExpressionToSettingNameListVisitor.convert(assignment)) {
                checkIfSettingIsRuntime(setting);
            }

            settingsApplier.apply(builder, parameterContext.parameters(), assignment.expression());
        }
        return new SetAnalyzedStatement(builder.build(), node.settingType().equals(SetStatement.SettingType.PERSISTENT));
    }

    public static ResetAnalyzedStatement analyze(ResetStatement node, ParameterContext parameterContext) {
        Set<String> settingsToRemove = Sets.newHashSet();
        for (Expression expression : node.columns()) {
            String settingsName = ExpressionToStringVisitor.convert(expression, parameterContext.parameters());
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
        checkIfSettingIsRuntime(CrateSettings.CRATE_SETTINGS, name);
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
        private ExpressionToSettingNameListVisitor() {}

        public static Collection<String> convert(Node node) {
            return INSTANCE.process(node, null);
        }

        @Override
        public Collection<String> visitAssignment(Assignment node, String context) {
            String left = ExpressionToStringVisitor.convert(node.columnName(), null);
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
