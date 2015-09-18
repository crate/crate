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
import io.crate.operation.NodeOperation;

import java.util.Collection;
import java.util.Map;

public class NodeOperationGrouper {

    public static final NodeOperationGrouper INSTANCE = new NodeOperationGrouper();

    public static class Context {

        private final ArrayListMultimap<String, NodeOperation> byServer;
        public NodeOperation currentOperation;

        public Context() {
            this.byServer = ArrayListMultimap.create();
        }

        protected void add(String server) {
            byServer.put(server, currentOperation);
        }

        public Map<String, Collection<NodeOperation>> grouped() {
            return byServer.asMap();
        }
    }

    public static Map<String, Collection<NodeOperation>> groupByServer(Iterable<NodeOperation> nodeOperations) {
        Context ctx = new Context();
        for (NodeOperation nodeOperation : nodeOperations) {
            ctx.currentOperation = nodeOperation;
            for (String server : nodeOperation.executionPhase().executionNodes()) {
                ctx.add(server);
            }
        }
        return ctx.grouped();
    }
}
