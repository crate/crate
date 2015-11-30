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

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.CopyFromStatement;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class CopyFromAnalyzer {

    private final AnalysisMetaData analysisMetaData;

    @Inject
    public CopyFromAnalyzer(AnalysisMetaData analysisMetaData) {
        this.analysisMetaData = analysisMetaData;
    }

    public CopyAnalyzedStatement convert(CopyFromStatement node, Analysis analysis) {
        CopyAnalyzedStatement statement = new CopyAnalyzedStatement();
        DocTableInfo tableInfo = analysisMetaData.referenceInfos().getWritableTable(
                TableIdent.of(node.table(), analysis.parameterContext().defaultSchema()));
        statement.table(tableInfo);
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);

        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData,
                analysis.parameterContext(),
                new NameFieldProvider(tableRelation),
                tableRelation);
        expressionAnalyzer.resolveWritableFields(true);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        Symbol pathSymbol = expressionAnalyzer.convert(node.path(), expressionAnalysisContext);
        statement.uri(pathSymbol);
        if (node.genericProperties().isPresent()) {
            statement.settings(CopyStatementAnalyzer.settingsFromProperties(
                    node.genericProperties().get(),
                    expressionAnalyzer,
                    expressionAnalysisContext));
        }
        statement.mode(CopyAnalyzedStatement.Mode.FROM);

        if (!node.table().partitionProperties().isEmpty()) {
            statement.partitionIdent(PartitionPropertiesAnalyzer.toPartitionIdent(
                    tableInfo,
                    node.table().partitionProperties(),
                    analysis.parameterContext().parameters()));
        }
        return statement;
    }
}
