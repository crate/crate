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

import io.crate.Constants;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CreateBlobTable;
import org.elasticsearch.cluster.metadata.IndexMetaData;

public class CreateBlobTableStatementAnalyzer extends BlobTableAnalyzer<CreateBlobTableAnalyzedStatement> {

    private static final TablePropertiesAnalyzer TABLE_PROPERTIES_ANALYZER = new TablePropertiesAnalyzer();


    @Override
    public Void visitCreateBlobTable(CreateBlobTable node, CreateBlobTableAnalyzedStatement context) {
        context.table(tableToIdent(node.name()));

        if (node.clusteredBy().isPresent()) {
            ClusteredBy clusteredBy = node.clusteredBy().get();

            int numShards;
            if (clusteredBy.numberOfShards().isPresent()) {
                numShards = ExpressionToNumberVisitor.convert(clusteredBy.numberOfShards().get(), context.parameters()).intValue();
                if (numShards < 1) {
                    throw new IllegalArgumentException("num_shards in CLUSTERED clause must be greater than 0");
                }
            } else {
                numShards = Constants.DEFAULT_NUM_SHARDS;
            }
            context.tableParameter().settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards);
        }


        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        TABLE_PROPERTIES_ANALYZER.analyze(
                context.tableParameter(), new BlobTableParameterInfo(),
                node.genericProperties(), context.parameters(), true);

        return null;
    }

    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new CreateBlobTableAnalyzedStatement(parameterContext);
    }
}
