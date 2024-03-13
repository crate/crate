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

import java.util.HashMap;
import java.util.List;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.ShardedTable;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.PromoteReplica;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.RerouteAllocateReplicaShard;
import io.crate.sql.tree.RerouteCancelShard;
import io.crate.sql.tree.RerouteMoveShard;
import io.crate.sql.tree.Table;

public class AlterTableRerouteAnalyzer {

    private final NodeContext nodeCtx;
    private final Schemas schemas;
    private final RerouteOptionVisitor rerouteOptionVisitor;

    AlterTableRerouteAnalyzer(NodeContext nodeCtx, Schemas schemas) {
        this.nodeCtx = nodeCtx;
        this.schemas = schemas;
        this.rerouteOptionVisitor = new RerouteOptionVisitor();
    }

    public AnalyzedStatement analyze(AlterTableReroute<Expression> alterTableReroute,
                                     ParamTypeHints paramTypeHints,
                                     CoordinatorTxnCtx transactionContext) {
        // safe to expect a `ShardedTable` since getTableInfo with
        // Operation.ALTER_REROUTE raises a appropriate error for sys tables
        ShardedTable tableInfo;
        Table<Expression> table = alterTableReroute.table();
        QualifiedName qName = alterTableReroute.blob()
            ? RelationName.fromBlobTable(table).toQualifiedName()
            : table.getName();

        CoordinatorSessionSettings sessionSettings = transactionContext.sessionSettings();
        tableInfo = schemas.resolveRelationInfo(
            qName,
            Operation.ALTER_REROUTE,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );
        return alterTableReroute.rerouteOption().accept(
            rerouteOptionVisitor,
            new Context(
                tableInfo,
                table.partitionProperties(),
                transactionContext,
                nodeCtx,
                paramTypeHints
            ));
    }

    private static class Context {

        private final ShardedTable tableInfo;
        private final List<Assignment<Expression>> partitionProperties;
        private final ExpressionAnalyzer exprAnalyzer;
        private final ExpressionAnalysisContext exprCtx;
        private final ExpressionAnalyzer exprAnalyzerWithFields;

        private Context(ShardedTable tableInfo,
                        List<Assignment<Expression>> partitionProperties,
                        CoordinatorTxnCtx txnCtx,
                        NodeContext nodeCtx,
                        ParamTypeHints paramTypeHints) {
            this.tableInfo = tableInfo;
            this.partitionProperties = partitionProperties;
            this.exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
            this.exprAnalyzer = new ExpressionAnalyzer(
                txnCtx, nodeCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
            this.exprAnalyzerWithFields = new ExpressionAnalyzer(
                txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);
        }
    }

    private static class RerouteOptionVisitor extends AstVisitor<RerouteAnalyzedStatement, Context> {

        @Override
        public RerouteAnalyzedStatement visitRerouteMoveShard(RerouteMoveShard<?> node, Context context) {
            return new AnalyzedRerouteMoveShard(
                context.tableInfo,
                Lists.map(
                    context.partitionProperties,
                    p -> p.map(a -> context.exprAnalyzerWithFields.convert(a, context.exprCtx))
                ),
                node.map(x -> context.exprAnalyzer.convert((Expression) x, context.exprCtx))
            );
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShard<?> node,
                                                                         Context context) {
            return new AnalyzedRerouteAllocateReplicaShard(
                context.tableInfo,
                Lists.map(
                    context.partitionProperties,
                    p -> p.map(a -> context.exprAnalyzerWithFields.convert(a, context.exprCtx))
                ),
                node.map(x -> context.exprAnalyzer.convert((Expression) x, context.exprCtx))
            );
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteCancelShard(RerouteCancelShard<?> node, Context context) {
            return new AnalyzedRerouteCancelShard(
                context.tableInfo,
                Lists.map(
                    context.partitionProperties,
                    p -> p.map(a -> context.exprAnalyzerWithFields.convert(a, context.exprCtx))
                ),
                node.map(x -> context.exprAnalyzer.convert((Expression) x, context.exprCtx))
            );
        }

        @Override
        public RerouteAnalyzedStatement visitReroutePromoteReplica(PromoteReplica<?> node, Context context) {
            var promoteReplica = node.map(x -> context.exprAnalyzer.convert((Expression) x, context.exprCtx));

            HashMap<String, Symbol> properties = new HashMap<>(promoteReplica.properties().properties());
            Symbol acceptDataLoss = properties.remove(PromoteReplica.Properties.ACCEPT_DATA_LOSS);
            if (!properties.isEmpty()) {
                throw new IllegalArgumentException(
                    "Unsupported options provided to REROUTE PROMOTE REPLICA: " + properties.keySet());
            }

            return new AnalyzedPromoteReplica(
                context.tableInfo,
                Lists.map(
                    context.partitionProperties,
                    p -> p.map(a -> context.exprAnalyzerWithFields.convert(a, context.exprCtx))
                ),
                node.map(x -> context.exprAnalyzer.convert((Expression) x, context.exprCtx)),
                acceptDataLoss == null ? Literal.BOOLEAN_FALSE : acceptDataLoss
            );
        }
    }
}
