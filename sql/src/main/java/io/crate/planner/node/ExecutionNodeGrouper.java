/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.node;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.node.dql.CollectNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExecutionNodeGrouper extends ExecutionNodeVisitor<ExecutionNodeGrouper.Context,Void> {

    public static final ExecutionNodeGrouper INSTANCE = new ExecutionNodeGrouper();

    public static class Context {

        private final String localNodeId;
        private final ArrayListMultimap<String, ExecutionNode> byServer;

        public Context(String localNodeId) {
            this.localNodeId = localNodeId;
            this.byServer = ArrayListMultimap.create();
        }

        protected void put(String server, ExecutionNode executionNode) {
            byServer.put(server, executionNode);
        }

        public Map<String, Collection<ExecutionNode>> grouped() {
            return byServer.asMap();
        }
    }

    public static Map<String, Collection<ExecutionNode>> groupByServer(String localNodeId,
                                                                       List<List<ExecutionNode>> groupedExecutionNodes) {
        Context ctx = new Context(localNodeId);
        for (List<ExecutionNode> group: groupedExecutionNodes) {
            for (ExecutionNode executionNode : group) {
                INSTANCE.process(executionNode, ctx);
            }
        }
        return ctx.grouped();
    }

    @Override
    public Void visitCollectNode(CollectNode node, Context context) {
        /**
         * routing might contain a NULL_NODE_ID if there is no specific node which contains the indices or shards.
         * This is the case in information_schema queries (each node has those tables...)
         * Or sys.shards (for unassigned shards)
         *
         * So the routing will be either:
         *
         * {
         *      "": { "information_schema.tables": null }
         * }
         *
         * in this case the query will be executed on the localNodeId
         *
         * or for sys.shards:
         *
         * {
         *      "": { "some_table": [0, 1] },
         *      "n1": { "some_table": [2] },
         *      "n2": { "some_table": [3] },
         * }
         *
         * In this case, the "unassigned shard collect" will be executed on either n1 or n2
         * depending on which entry appears first in the executionNodes set.
         */

        Set<String> executionNodes = node.executionNodes();
        if (executionNodes.isEmpty()) {
            return null;
        }
        if (node.routing().isNullRouting()) {
            node.handlerSideCollect(context.localNodeId);
            context.put(context.localNodeId, node);
            return null;
        }

        for (String server : executionNodes) {
            context.put(server, node);
        }
        Map<String, Map<String, List<Integer>>> locations = node.routing().locations();
        if (locations != null && locations.containsKey(TableInfo.NULL_NODE_ID)) {
            node.handlerSideCollect(executionNodes.iterator().next());
        }
        return null;
    }

    @Override
    protected Void visitExecutionNode(ExecutionNode node, Context context) {
        for (String server : node.executionNodes()) {
            context.put(server, node);
        }
        return null;
    }
}
