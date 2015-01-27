/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.metadata.ReferenceInfos;
import io.crate.sql.tree.AlterBlobTable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class AlterBlobTableAnalyzer extends BlobTableAnalyzer<AlterBlobTableAnalyzedStatement> {

    private static final TablePropertiesAnalyzer TABLE_PROPERTIES_ANALYZER = new TablePropertiesAnalyzer();
    private final ReferenceInfos referenceInfos;

    @Inject
    public AlterBlobTableAnalyzer(ReferenceInfos referenceInfos) {
        this.referenceInfos = referenceInfos;
    }

    @Override
    public AlterBlobTableAnalyzedStatement visitAlterBlobTable(AlterBlobTable node, Analysis analysis) {
        AlterBlobTableAnalyzedStatement statement = new AlterBlobTableAnalyzedStatement(referenceInfos);

        statement.table(tableToIdent(node.table()));

        if (node.genericProperties().isPresent()) {
            TABLE_PROPERTIES_ANALYZER.analyze(
                    statement.tableParameter(), statement.table().tableParameterInfo(),
                    node.genericProperties(), analysis.parameterContext().parameters());
        } else if (!node.resetProperties().isEmpty()) {
            TABLE_PROPERTIES_ANALYZER.analyze(
                    statement.tableParameter(), statement.table().tableParameterInfo(),
                    node.resetProperties());
        }

        return statement;
    }

}
