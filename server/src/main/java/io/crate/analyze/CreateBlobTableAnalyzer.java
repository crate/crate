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

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.exceptions.scoped.table.RelationAlreadyExists;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.Expression;

public class CreateBlobTableAnalyzer {

    private final Schemas schemas;
    private final NodeContext nodeCtx;

    public CreateBlobTableAnalyzer(Schemas schemas, NodeContext nodeCtx) {
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedCreateBlobTable analyze(CreateBlobTable<Expression> node,
                                           ParamTypeHints paramTypeHints,
                                           CoordinatorTxnCtx txnCtx) {
        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());

        CreateBlobTable<Symbol> createBlobTable = node.map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx));

        RelationName relationName = RelationName.fromBlobTable(createBlobTable.name());
        relationName.ensureValidForRelationCreation();
        if (schemas.tableExists(relationName)) {
            throw new RelationAlreadyExists(relationName);
        }

        return new AnalyzedCreateBlobTable(relationName, createBlobTable);
    }
}
