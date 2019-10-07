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
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.ShardedTable;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.PromoteReplica;
import io.crate.sql.tree.RerouteAllocateReplicaShard;
import io.crate.sql.tree.RerouteCancelShard;
import io.crate.sql.tree.RerouteMoveShard;

import java.util.HashMap;
import java.util.List;

import static io.crate.metadata.RelationName.fromBlobTable;

public class AlterTableRerouteAnalyzer {

    private final RerouteOptionVisitor rerouteOptionVisitor;
    private final Schemas schemas;

    AlterTableRerouteAnalyzer(Functions functions, Schemas schemas) {
        this.schemas = schemas;
        this.rerouteOptionVisitor = new RerouteOptionVisitor(functions);
    }

    public AnalyzedStatement analyze(AlterTableReroute node, Analysis context) {
        // safe to expect a `ShardedTable` since getTableInfo with Operation.ALTER_REROUTE raises a appropriate error for sys tables
        ShardedTable tableInfo;
        RelationName relationName;
        if (node.blob()) {
            relationName = fromBlobTable(node.table());
        } else {
            relationName = schemas.resolveRelation(node.table().getName(), context.sessionContext().searchPath());
        }
        tableInfo = schemas.getTableInfo(relationName, Operation.ALTER_REROUTE);
        return node.rerouteOption().accept(rerouteOptionVisitor, new Context(
            tableInfo,
            node.table().partitionProperties(),
            context.transactionContext(),
            context.paramTypeHints()
        ));
    }

    private static class Context {

        private final ShardedTable tableInfo;
        private final List<Assignment<Expression>> partitionProperties;
        private final CoordinatorTxnCtx txnCtx;
        private final ParamTypeHints paramTypeHints;

        private Context(ShardedTable tableInfo,
                        List<Assignment<Expression>> partitionProperties,
                        CoordinatorTxnCtx txnCtx,
                        ParamTypeHints paramTypeHints) {
            this.tableInfo = tableInfo;
            this.partitionProperties = partitionProperties;
            this.txnCtx = txnCtx;
            this.paramTypeHints = paramTypeHints;
        }
    }

    private static class RerouteOptionVisitor extends AstVisitor<RerouteAnalyzedStatement, Context> {

        private final Functions functions;

        RerouteOptionVisitor(Functions functions) {
            this.functions = functions;
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteMoveShard(RerouteMoveShard node, Context context) {
            return new RerouteMoveShardAnalyzedStatement(
                context.tableInfo, context.partitionProperties, node.shardId(), node.fromNodeIdOrName(), node.toNodeIdOrName());
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShard node, Context context) {
            return new RerouteAllocateReplicaShardAnalyzedStatement(
                context.tableInfo, context.partitionProperties, node.shardId(), node.nodeIdOrName());
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteCancelShard(RerouteCancelShard node, Context context) {
            return new RerouteCancelShardAnalyzedStatement(
                context.tableInfo, context.partitionProperties, node.shardId(), node.nodeIdOrName(), node.properties());
        }

        @Override
        public RerouteAnalyzedStatement visitReroutePromoteReplica(PromoteReplica promoteReplica, Context context) {
            HashMap<String, Expression> properties = new HashMap<>(promoteReplica.properties().properties());
            Expression acceptDataLossExpr = properties.remove(PromoteReplica.Properties.ACCEPT_DATA_LOSS);
            if (!properties.isEmpty()) {
                throw new IllegalArgumentException("Unsupported options provided to REROUTE PROMOTE REPLICA: " + properties.keySet());
            }
            var exprCtx = new ExpressionAnalysisContext();
            var expressionAnalyzer = new ExpressionAnalyzer(
                functions, context.txnCtx, context.paramTypeHints, FieldProvider.UNSUPPORTED, null);

            Symbol acceptDataLoss = acceptDataLossExpr == null
                ? Literal.BOOLEAN_FALSE
                : expressionAnalyzer.convert(acceptDataLossExpr, exprCtx);
            Symbol shardId = expressionAnalyzer.convert(promoteReplica.shardId(), exprCtx);
            Symbol node = expressionAnalyzer.convert(promoteReplica.node(), exprCtx);
            return new PromoteReplicaStatement(
                context.tableInfo,
                context.partitionProperties,
                node,
                shardId,
                acceptDataLoss
            );
        }
    }
}
