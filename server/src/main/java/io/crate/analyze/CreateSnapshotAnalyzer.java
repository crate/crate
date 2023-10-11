/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.List;
import java.util.Locale;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists2;
import io.crate.execution.ddl.RepositoryService;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.sql.tree.CreateSnapshot;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Table;

class CreateSnapshotAnalyzer {

    private final RepositoryService repositoryService;
    private final NodeContext nodeCtx;

    CreateSnapshotAnalyzer(RepositoryService repositoryService, NodeContext nodeCtx) {
        this.repositoryService = repositoryService;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedCreateSnapshot analyze(CreateSnapshot<Expression> createSnapshot,
                                          ParamTypeHints paramTypeHints,
                                          CoordinatorTxnCtx txnCtx) {
        String repositoryName = createSnapshot.name().getPrefix()
            .map(name -> {
                validateRepository(name);
                return name.toString();
            })
            .orElseThrow(() -> new IllegalArgumentException(
                "Snapshot must be specified by \"<repository_name>\".\"<snapshot_name>\""));

        String snapshotName = createSnapshot.name().getSuffix();

        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);

        List<Table<Symbol>> tables = Lists2.map(
            createSnapshot.tables(),
            (table) -> table.map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx)));
        GenericProperties<Symbol> properties = createSnapshot.properties()
            .map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx));

        return new AnalyzedCreateSnapshot(repositoryName, snapshotName, tables, properties);
    }

    private void validateRepository(QualifiedName name) {
        if (name.getParts().size() != 1) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Invalid repository name '%s'", name)
            );
        }
        repositoryService.failIfRepositoryDoesNotExist(name.toString());
    }
}
