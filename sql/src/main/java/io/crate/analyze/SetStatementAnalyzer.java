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
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.analyze.relations.FieldProvider;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.settings.CrateSettings;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.ResetStatement;
import io.crate.sql.tree.SetStatement;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

class SetStatementAnalyzer {

    private static final Logger logger = Loggers.getLogger(SetStatementAnalyzer.class);

    private final Functions functions;

    SetStatementAnalyzer(Functions functions) {
        this.functions = functions;
    }

    public static SetAnalyzedStatement analyze(SetStatement node) {
        assert !SetStatement.Scope.LICENSE.equals(node.scope())  : "cannot analyze statements of scope: LICENSE";

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

    public SetLicenseAnalyzedStatement analyze(SetStatement node, Analysis context) {
        assert SetStatement.Scope.LICENSE.equals(node.scope()) : "Should only analyze statements of scope: LICENSE";

        if (node.assignments().size() != SetLicenseAnalyzedStatement.LICENSE_TOKEN_NUM) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Invalid number of settings for SET LICENSE. Please provide the following settings: [%s]",
                SetLicenseAnalyzedStatement.LICENSE_ALLOWED_TOKENS_AS_STRING));
        }

        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            context.transactionContext(),
            context.paramTypeHints(),
            FieldProvider.UNSUPPORTED,
            null);
        Map<String, Symbol> statementSymbols = new HashMap<>(SetLicenseAnalyzedStatement.LICENSE_TOKEN_NUM);
        ExpressionAnalysisContext exprCtx = new ExpressionAnalysisContext();

        for (Assignment assignment : node.assignments()) {
            String settingName = ExpressionToStringVisitor.convert(assignment.columnName(), Row.EMPTY);
            if (!SetLicenseAnalyzedStatement.LICENSE_ALLOWED_TOKENS.contains(settingName)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Invalid setting '%s' for SET LICENSE. Please provide the following settings: [%s]",
                    settingName, SetLicenseAnalyzedStatement.LICENSE_ALLOWED_TOKENS_AS_STRING));
            }
            Symbol valueSymbol = expressionAnalyzer.convert(assignment.expression(), exprCtx);
            statementSymbols.put(settingName, valueSymbol);
        }
        return new SetLicenseAnalyzedStatement(statementSymbols.get(SetLicenseAnalyzedStatement.EXPIRY_DATE_TOKEN),
            statementSymbols.get(SetLicenseAnalyzedStatement.ISSUED_TO_TOKEN),
            statementSymbols.get(SetLicenseAnalyzedStatement.SIGN_TOKEN));
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
