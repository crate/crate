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

import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.ShowCreateTable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class ShowCreateTableAnalyzer extends DefaultTraversalVisitor<ShowCreateTableAnalyzedStatement, Analysis> {

    private final AnalysisMetaData analysisMetaData;

    public AnalyzedStatement analyze(Node node, Analysis analysis) {
        return super.process(node, analysis);
    }

    @Inject
    public ShowCreateTableAnalyzer(AnalysisMetaData analysisMetaData) {
        this.analysisMetaData = analysisMetaData;
    }

    @Override
    public ShowCreateTableAnalyzedStatement visitShowCreateTable(ShowCreateTable node, Analysis analysis) {
        TableIdent tableIdent = TableIdent.of(node.table(), analysis.parameterContext().defaultSchema());
        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfo(tableIdent);

        if (tableInfo.schemaInfo().systemSchema() &&
                !tableInfo.schemaInfo().name().equals(ReferenceInfos.DEFAULT_SCHEMA_NAME)) {
            throw new UnsupportedOperationException("Table must be a doc table");
        }

        DocTableInfo info = (DocTableInfo) tableInfo;
        ShowCreateTableAnalyzedStatement analyzedStatement = new ShowCreateTableAnalyzedStatement(info);
        analysis.rootRelation(analyzedStatement);
        return analyzedStatement;
    }

}
