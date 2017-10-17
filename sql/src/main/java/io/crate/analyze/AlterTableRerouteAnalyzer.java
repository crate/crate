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

import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.ShardedTable;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.RerouteAllocateReplicaShard;
import io.crate.sql.tree.RerouteCancelShard;
import io.crate.sql.tree.RerouteMoveShard;
import io.crate.sql.tree.RerouteRetryFailed;

public class AlterTableRerouteAnalyzer {

    private static final RerouteOptionVisitor REROUTE_OPTION_VISITOR = new RerouteOptionVisitor();
    private final Schemas schemas;

    AlterTableRerouteAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public AnalyzedStatement analyze(AlterTableReroute node, Analysis context) {
        ShardedTable tableInfo = schemas.getTableInfo(
            TableIdent.of(node.table(), context.sessionContext().defaultSchema()),
            Operation.ALTER_REROUTE);
        return REROUTE_OPTION_VISITOR.process(node.rerouteOption(), tableInfo.concreteIndices());
    }

    private static class RerouteOptionVisitor extends AstVisitor<RerouteAnalyzedStatement, String[]> {

        @Override
        public RerouteAnalyzedStatement visitRerouteMoveShard(RerouteMoveShard node, String[] context) {
            return new RerouteMoveShardAnalyzedStatement(context, node.shardId(), node.fromNodeId(), node.toNodeId());
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteCancelShard(RerouteCancelShard node, String[] context) {
            return new RerouteCancelShardAnalyzedStatement(context, node.shardId(), node.nodeId(), node.properties());
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShard node, String[] context) {
            return new RerouteAllocateReplicaShardAnalyzedStatement(context, node.shardId(), node.nodeId());
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteRetryFailed(RerouteRetryFailed node, String[] context) {
            return new RerouteRetryFailedAnalyzedStatement(context);
        }
    }
}
