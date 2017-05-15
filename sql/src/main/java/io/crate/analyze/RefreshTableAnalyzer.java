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
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.operation.user.User;
import io.crate.sql.tree.RefreshStatement;
import io.crate.sql.tree.Table;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class RefreshTableAnalyzer {

    private final Schemas schemas;

    RefreshTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public RefreshTableAnalyzedStatement analyze(RefreshStatement refreshStatement, Analysis analysis) {
        return new RefreshTableAnalyzedStatement(getIndexNames(
            refreshStatement.tables(),
            schemas,
            analysis.parameterContext(),
            analysis.sessionContext().defaultSchema(),
            analysis.sessionContext().user()
        ));
    }

    private static Set<String> getIndexNames(List<Table> tables,
                                             Schemas schemas,
                                             ParameterContext parameterContext,
                                             String defaultSchema,
                                             User user) {
        Set<String> indexNames = new HashSet<>(tables.size());
        for (Table nodeTable : tables) {
            DocTableInfo tableInfo = schemas.getTableInfo(
                TableIdent.of(nodeTable, defaultSchema), Operation.REFRESH, user);
            indexNames.addAll(TableAnalyzer.filteredIndices(
                    parameterContext,
                    nodeTable.partitionProperties(), tableInfo));
        }
        return indexNames;
    }
}
