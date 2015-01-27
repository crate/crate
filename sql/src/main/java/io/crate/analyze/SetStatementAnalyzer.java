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
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.metadata.settings.CrateSettings;
import io.crate.sql.tree.*;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.Locale;
import java.util.Set;

@Singleton
public class SetStatementAnalyzer extends DefaultTraversalVisitor<SetAnalyzedStatement, Analysis> {

    private final ESLogger logger = Loggers.getLogger(this.getClass());

    public SetAnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return super.process(node, analysis);
    }

    @Override
    public SetAnalyzedStatement visitSetStatement(SetStatement node, Analysis analysis) {
        SetAnalyzedStatement statement = new SetAnalyzedStatement();
        statement.persistent(node.settingType().equals(SetStatement.SettingType.PERSISTENT));
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        for (Assignment assignment : node.assignments()) {
            String settingsName = ExpressionToStringVisitor.convert(assignment.columnName(),
                    analysis.parameterContext().parameters());
            SettingsApplier settingsApplier = CrateSettings.getSetting(settingsName);
            if (settingsApplier == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "setting '%s' not supported", settingsName));
            }
            settingsApplier.apply(builder, analysis.parameterContext().parameters(), assignment.expression());
        }
        statement.settings(builder.build());
        return statement;
    }

    @Override
    public SetAnalyzedStatement visitResetStatement(ResetStatement node, Analysis analysis) {
        SetAnalyzedStatement statement = new SetAnalyzedStatement();
        statement.isReset(true);
        Set<String> settingsToRemove = Sets.newHashSet();
        for (Expression expression : node.columns()) {
            String settingsName = ExpressionToStringVisitor.convert(expression, analysis.parameterContext().parameters());
            if (!settingsToRemove.contains(settingsName)) {
                Set<String> settingNames = CrateSettings.settingNamesByPrefix(settingsName);
                if (settingNames.size() == 0) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "setting '%s' not supported", settingsName));
                }
                settingsToRemove.addAll(settingNames);
                logger.info("resetting []", settingNames);
            }
        }
        statement.settingsToRemove(settingsToRemove);
        return statement;
    }
}
