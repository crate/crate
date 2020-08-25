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
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists2;
import io.crate.execution.ddl.RepositoryService;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.RestoreSnapshot;
import io.crate.sql.tree.Table;

import java.util.List;

class RestoreSnapshotAnalyzer {

    private final RepositoryService repositoryService;
    private final NodeContext nodeCtx;

    RestoreSnapshotAnalyzer(RepositoryService repositoryService, NodeContext nodeCtx) {
        this.repositoryService = repositoryService;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedRestoreSnapshot analyze(RestoreSnapshot<Expression> restoreSnapshot,
                                           ParamTypeHints paramTypeHints,
                                           CoordinatorTxnCtx txnCtx) {
        List<String> nameParts = restoreSnapshot.name().getParts();
        if (nameParts.size() != 2) {
            throw new IllegalArgumentException(
                "Snapshot name not supported, only <repository>.<snapshot> works.)");
        }
        var repositoryName = nameParts.get(0);
        var snapshotName = nameParts.get(1);
        repositoryService.failIfRepositoryDoesNotExist(repositoryName);

        var exprCtx = new ExpressionAnalysisContext();
        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.FIELDS_AS_LITERAL, null);

        List<Table<Symbol>> tables = Lists2.map(
            restoreSnapshot.tables(),
            (table) -> table.map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx)));
        GenericProperties<Symbol> properties = restoreSnapshot.properties()
            .map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx));

        return new AnalyzedRestoreSnapshot(repositoryName, snapshotName, tables, properties);
    }
}
