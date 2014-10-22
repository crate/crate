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
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;

public class AlterBlobTableAnalyzer extends BlobTableAnalyzer<AlterBlobTableAnalysis> {

    private static final TablePropertiesAnalysis tablePropertiesAnalysis = new AlterBlobTablePropertiesAnalysis();
    private final ReferenceInfos referenceInfos;

    @Inject
    public AlterBlobTableAnalyzer(ReferenceInfos referenceInfos) {
        this.referenceInfos = referenceInfos;
    }

    @Override
    public Void visitAlterBlobTable(AlterBlobTable node, AlterBlobTableAnalysis context) {
        context.table(tableToIdent(node.table()));

        if (node.genericProperties().isPresent()) {
            GenericProperties properties = node.genericProperties().get();
            TablePropertiesAnalysis.TableProperties tableProperties =
                    tablePropertiesAnalysis.tableProperties(properties, context.parameters());
            context.indexSettingsBuilder().put(tableProperties.settings());
        } else if (!node.resetProperties().isEmpty()) {
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            for (String property : node.resetProperties()) {
                builder.put(tablePropertiesAnalysis.getDefault(property));
            }
            context.indexSettingsBuilder().put(builder.build());
        }

        return null;
    }

    @Override
    public Analysis newAnalysis(Analyzer.ParameterContext parameterContext) {
        return new AlterBlobTableAnalysis(parameterContext, referenceInfos);
    }
}
