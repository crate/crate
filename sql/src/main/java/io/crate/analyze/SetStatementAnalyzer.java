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
import io.crate.data.Row;
import io.crate.metadata.settings.CrateSettings;
import io.crate.sql.tree.*;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;

class SetStatementAnalyzer {

    private static final Logger logger = Loggers.getLogger(SetStatementAnalyzer.class);

    public static SetAnalyzedStatement analyze(SetStatement node) {
        boolean isPersistent = node.settingType().equals(SetStatement.SettingType.PERSISTENT);
        Map<String, List<Expression>> settings = new HashMap<>();

        if (!SetStatement.Scope.GLOBAL.equals(node.scope())) {
            Assignment assignment = node.assignments().get(0);
            // parser does not allow using the parameter expressions as setting names in the SET statements,
            // therefore it is fine to convert the expression to string here.
            String settingName = ExpressionToStringVisitor.convert(assignment.columnName(), Row.EMPTY);
            List<String> nameParts = CrateSettings.settingNamesByPrefix(settingName);
            if (nameParts.size() != 0) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "GLOBAL Cluster setting '%s' cannot be used with SET SESSION / LOCAL", settingName));
            }
            settings.put(settingName, assignment.expressions());
            return new SetAnalyzedStatement(node.scope(), settings, isPersistent);
        } else {
            for (Assignment assignment : node.assignments()) {
                for (String setting : ExpressionToSettingNameListVisitor.convert(assignment)) {
                    CrateSettings.checkIfRuntimeSetting(setting);
                }
                String settingName = ExpressionToStringVisitor.convert(assignment.columnName(), Row.EMPTY);
                settings.put(settingName, ImmutableList.of(assignment.expression()));
            }
            return new SetAnalyzedStatement(node.scope(), settings, isPersistent);
        }
    }

    public static ResetAnalyzedStatement analyze(ResetStatement node) {
        Set<String> settingsToRemove = Sets.newHashSet();
        for (Expression expression : node.columns()) {
            String settingsName = ExpressionToStringVisitor.convert(expression, Row.EMPTY);
            if (!settingsToRemove.contains(settingsName)) {
                List<String> settingNames = CrateSettings.settingNamesByPrefix(settingsName);
                if (settingNames.size() == 0) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Setting '%s' not supported", settingsName));
                }
                for (String setting : settingNames) {
                    CrateSettings.checkIfRuntimeSetting(setting);
                }
                settingsToRemove.addAll(settingNames);
                logger.info("resetting [{}]", settingNames);
            }
        }
        return new ResetAnalyzedStatement(settingsToRemove);
    }

    private static class ExpressionToSettingNameListVisitor extends AstVisitor<Collection<String>, String> {

        private static final ExpressionToSettingNameListVisitor INSTANCE = new ExpressionToSettingNameListVisitor();

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
