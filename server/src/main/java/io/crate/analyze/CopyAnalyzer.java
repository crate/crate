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
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.TableRelation;
import io.crate.common.collections.Lists;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.CopyTo;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;

class CopyAnalyzer {

    private final Schemas schemas;
    private final NodeContext nodeCtx;

    CopyAnalyzer(Schemas schemas, NodeContext nodeCtx) {
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
    }

    AnalyzedCopyFrom analyzeCopyFrom(CopyFrom<Expression> node,
                                     ParamTypeHints paramTypeHints,
                                     CoordinatorTxnCtx txnCtx) {
        CoordinatorSessionSettings sessionSettings = txnCtx.sessionSettings();
        DocTableInfo tableInfo = schemas.resolveRelationInfo(
            node.table().getName(),
            Operation.INSERT,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );
        var exprCtx = new ExpressionAnalysisContext(sessionSettings);

        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);

        var normalizer = new EvaluatingNormalizer(
            nodeCtx,
            RowGranularity.CLUSTER,
            null,
            new TableRelation(tableInfo));

        Table<Symbol> table = node.table().map(t -> exprAnalyzerWithFieldsAsString.convert(t, exprCtx));
        GenericProperties<Symbol> properties = node.properties().map(t -> exprAnalyzerWithoutFields.convert(t,
                                                                                                            exprCtx));
        Symbol uri = exprAnalyzerWithoutFields.convert(node.path(), exprCtx);

        if (node.isReturnSummary()) {
            return new AnalyzedCopyFromReturnSummary(
                tableInfo,
                node.columns(),
                table,
                properties,
                normalizer.normalize(uri, txnCtx));
        } else {
            return new AnalyzedCopyFrom(
                tableInfo,
                node.columns(),
                table,
                properties,
                normalizer.normalize(uri, txnCtx));
        }
    }

    AnalyzedCopyTo analyzeCopyTo(CopyTo<Expression> node,
                                 ParamTypeHints paramTypeHints,
                                 CoordinatorTxnCtx txnCtx) {
        if (!node.directoryUri()) {
            throw new UnsupportedOperationException("Using COPY TO without specifying a DIRECTORY is not supported");
        }

        DocTableInfo tableInfo = schemas.resolveRelationInfo(
            node.table().getName(),
            Operation.COPY_TO,
            txnCtx.sessionSettings().sessionUser(),
            txnCtx.sessionSettings().searchPath()
        );
        Operation.blockedRaiseException(tableInfo, Operation.READ);
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);

        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            nodeCtx,
            RowGranularity.CLUSTER,
            null,
            tableRelation);

        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        var expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            new NameFieldProvider(tableRelation),
            null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            FieldProvider.TO_LITERAL_VALIDATE_NAME,
            null);

        var uri = expressionAnalyzer.convert(node.targetUri(), exprCtx);
        var table = node.table().map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx));
        var properties = node.properties().map(x -> expressionAnalyzer.convert(x, exprCtx));
        var columns = Lists.map(
            node.columns(),
            c -> normalizer.normalize(expressionAnalyzer.convert(c, exprCtx), txnCtx));
        var whereClause = node.whereClause().map(
            w -> normalizer.normalize(expressionAnalyzer.convert(w, exprCtx), txnCtx)).orElse(null);

        return new AnalyzedCopyTo(
            tableInfo,
            table,
            normalizer.normalize(uri, txnCtx),
            properties,
            columns,
            whereClause);
    }
}
