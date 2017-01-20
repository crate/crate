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

import io.crate.metadata.Schemas;
import io.crate.sql.tree.AlterBlobTable;

import static io.crate.analyze.BlobTableAnalyzer.tableToIdent;

class AlterBlobTableAnalyzer {

    private final Schemas schemas;

    AlterBlobTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public AlterBlobTableAnalyzedStatement analyze(AlterBlobTable node, ParameterContext parameterContext) {
        AlterBlobTableAnalyzedStatement statement = new AlterBlobTableAnalyzedStatement(schemas);

        statement.table(tableToIdent(node.table()));

        if (node.genericProperties().isPresent()) {
            TablePropertiesAnalyzer.analyze(
                statement.tableParameter(), statement.table().tableParameterInfo(),
                node.genericProperties(), parameterContext.parameters());
        } else if (!node.resetProperties().isEmpty()) {
            TablePropertiesAnalyzer.analyze(
                statement.tableParameter(), statement.table().tableParameterInfo(),
                node.resetProperties());
        }
        return statement;
    }
}
