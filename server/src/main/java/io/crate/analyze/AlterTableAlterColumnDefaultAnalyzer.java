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
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.planner.operators.EnsureNoMatchPredicate;
import io.crate.sql.tree.AlterTableAlterColumnDefault;
import io.crate.sql.tree.Expression;
import io.crate.types.ObjectType;

public class AlterTableAlterColumnDefaultAnalyzer {

    private final Schemas schemas;
    private final NodeContext nodeCtx;

    public AlterTableAlterColumnDefaultAnalyzer(Schemas schemas, NodeContext nodeCtx) {
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedAlterTableAlterColumnDefault analyze(AlterTableAlterColumnDefault<Expression> node,
                                                        ParamTypeHints paramTypeHints,
                                                        CoordinatorTxnCtx txnCtx) {
        if (!node.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException(
                "Altering a column default from a single partition is not supported");
        }

        CoordinatorSessionSettings sessionSettings = txnCtx.sessionSettings();
        DocTableInfo tableInfo = schemas.findRelation(
            node.table().getName(),
            Operation.ALTER,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );

        if (tableInfo.versionCreated().before(DocTableInfo.COLUMN_OID_VERSION)) {
            throw new UnsupportedOperationException(
                "Altering the default of columns of a table created before version "
                    + DocTableInfo.COLUMN_OID_VERSION + " is not supported"
            );
        }

        var expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            new AlterTableRenameColumnAnalyzer.FieldProviderResolvesUnknownColumns(tableInfo),
            null
        );
        var expressionContext = new ExpressionAnalysisContext(sessionSettings);

        Reference ref = (Reference) expressionAnalyzer.convert(node.column(), expressionContext);

        if (ref.isGenerated()) {
            throw new IllegalArgumentException(
                "Cannot SET DEFAULT on generated column `" + ref.column().fqn() + "`"
            );
        }
        if (ref.column().isSystemColumn()) {
            throw new IllegalArgumentException(
                "Cannot alter system column `" + ref.column().fqn() + "`"
            );
        }
        if (ref.valueType().id() == ObjectType.ID) {
            throw new IllegalArgumentException(
                "Default values are not allowed for object columns: " + ref.column().fqn()
            );
        }

        Symbol newDefault = null;
        if (node.defaultExpression() != null) {
            newDefault = expressionAnalyzer.convert(node.defaultExpression(), expressionContext);
            newDefault = newDefault.cast(ref.valueType(), CastMode.IMPLICIT);
            // Validate by normalizing; result is not stored to preserve functions like current_timestamp
            var normalizer = EvaluatingNormalizer.functionOnlyNormalizer(nodeCtx);
            normalizer.normalize(newDefault, txnCtx);
            newDefault.visit(Reference.class, x -> {
                throw new UnsupportedOperationException(
                    "Cannot reference columns in DEFAULT expression of `" + ref.column().fqn() + "`. " +
                        "Maybe you wanted to use a string literal with single quotes instead: '" + x.column().name() + "'");
            });
            EnsureNoMatchPredicate.ensureNoMatchPredicate(newDefault, "Cannot use MATCH in DEFAULT expression");
        }

        return new AnalyzedAlterTableAlterColumnDefault(tableInfo.ident(), ref, newDefault);
    }
}
