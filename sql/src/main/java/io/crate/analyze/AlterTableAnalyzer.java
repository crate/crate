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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.AlterTableOpenClose;
import io.crate.sql.tree.AlterTableRename;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.Table;

import java.util.List;
import java.util.function.Function;

import static io.crate.analyze.BlobTableAnalyzer.tableToIdent;

class AlterTableAnalyzer {

    private final Schemas schemas;
    private final Functions functions;

    AlterTableAnalyzer(Schemas schemas, Functions functions) {
        this.schemas = schemas;
        this.functions = functions;
    }

    AnalyzedAlterTable analyze(AlterTable<Expression> node,
                               Function<ParameterExpression, Symbol> convertParamFunction,
                               CoordinatorTxnCtx txnCtx) {
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            functions, txnCtx, convertParamFunction, FieldProvider.FIELDS_AS_LITERAL, null);
        var exprCtx = new ExpressionAnalysisContext();

        AlterTable<Symbol> alterTable = node.map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx));

        DocTableInfo docTableInfo = (DocTableInfo) schemas.resolveTableInfo(alterTable.table().getName(), Operation.ALTER_BLOCKS,
            txnCtx.sessionContext().searchPath());

        return new AnalyzedAlterTable(docTableInfo, alterTable);
    }

    AnalyzedAlterTableRename analyze(AlterTableRename<Expression> node, SessionContext sessionContext) {
        if (!node.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Renaming a single partition is not supported");
        }

        // we do not support renaming to a different schema, thus the target table identifier must not include a schema
        // this is an artificial limitation, technically it can be done
        List<String> newIdentParts = node.newName().getParts();
        if (newIdentParts.size() > 1) {
            throw new IllegalArgumentException("Target table name must not include a schema");
        }

        RelationName relationName;
        if (node.blob()) {
            relationName = tableToIdent(node.table());
        } else {
            relationName = schemas.resolveRelation(node.table().getName(), sessionContext.searchPath());
        }

        DocTableInfo tableInfo = schemas.getTableInfo(relationName, Operation.ALTER_TABLE_RENAME);
        RelationName newRelationName = new RelationName(relationName.schema(), newIdentParts.get(0));
        newRelationName.ensureValidForRelationCreation();
        return new AnalyzedAlterTableRename(tableInfo, newRelationName);
    }

    public AnalyzedAlterTableOpenClose analyze(AlterTableOpenClose<Expression> node,
                                               Function<ParameterExpression, Symbol> convertParamFunction,
                                               CoordinatorTxnCtx txnCtx) {
        var exprAnalyzerWithFieldsAsStrings = new ExpressionAnalyzer(
            functions, txnCtx, convertParamFunction, FieldProvider.FIELDS_AS_LITERAL, null);
        var exprCtx = new ExpressionAnalysisContext();

        Table<Symbol> table = node.table().map(x -> exprAnalyzerWithFieldsAsStrings.convert(x, exprCtx));
        RelationName relationName;
        if (node.blob()) {
            relationName = tableToIdent(table);
        } else {
            relationName = schemas.resolveRelation(table.getName(), txnCtx.sessionContext().searchPath());
        }

        DocTableInfo tableInfo = schemas.getTableInfo(relationName, Operation.ALTER_OPEN_CLOSE);
        return new AnalyzedAlterTableOpenClose(tableInfo, table, node.openTable());
    }
}
