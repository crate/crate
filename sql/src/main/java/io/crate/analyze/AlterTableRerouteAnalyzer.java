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
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.RerouteAllocateReplicaShard;
import io.crate.sql.tree.RerouteMoveShard;

import java.util.List;

public class AlterTableRerouteAnalyzer {

    private static final RerouteOptionVisitor REROUTE_OPTION_VISITOR = new RerouteOptionVisitor();
    private final Schemas schemas;

    AlterTableRerouteAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public AnalyzedStatement analyze(AlterTableReroute node, Analysis context) {
        // safe to expect a `ShardedTable` since REROUTE operation is not allowed on SYS tables at all.
        ShardedTable tableInfo = schemas.getTableInfo(
            TableIdent.of(node.table(), context.sessionContext().defaultSchema()),
            Operation.ALTER_REROUTE);
        return REROUTE_OPTION_VISITOR.process(node.rerouteOption(), new Context(tableInfo, node.table().partitionProperties()));
    }

    private class Context {

        final ShardedTable tableInfo;
        final List<Assignment> partitionProperties;

        private Context(ShardedTable tableInfo, List<Assignment> partitionProperties) {
            this.tableInfo = tableInfo;
            this.partitionProperties = partitionProperties;
        }
    }

    private static class RerouteOptionVisitor extends AstVisitor<RerouteAnalyzedStatement, Context> {

        @Override
        public RerouteAnalyzedStatement visitRerouteMoveShard(RerouteMoveShard node, Context context) {
            return new RerouteMoveShardAnalyzedStatement(
                context.tableInfo, context.partitionProperties, node.shardId(),node.fromNodeId(), node.toNodeId());
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShard node, Context context) {
            return new RerouteAllocateReplicaShardAnalyzedStatement(
                context.tableInfo, context.partitionProperties, node.shardId(), node.nodeId());
        }
    }
}
