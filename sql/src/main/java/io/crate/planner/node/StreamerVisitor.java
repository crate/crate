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

package io.crate.planner.node;

import io.crate.Streamer;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.types.DataTypes;

/**
 * get input and output {@link io.crate.Streamer}s for {@link io.crate.planner.node.PlanNode}s
 */
public class StreamerVisitor {

    private static final ExecutionPhaseStreamerVisitor EXECUTION_PHASE_STREAMER_VISITOR = new ExecutionPhaseStreamerVisitor();

    private StreamerVisitor() {}

    public static Streamer<?>[] streamerFromOutputs(ExecutionPhase executionPhase) {
        return EXECUTION_PHASE_STREAMER_VISITOR.process(executionPhase, null);
    }

    private static class ExecutionPhaseStreamerVisitor extends ExecutionPhaseVisitor<Void, Streamer<?>[]> {

        @Override
        public Streamer<?>[] visitMergeNode(MergePhase node, Void context) {
            return DataTypes.getStreamer(node.outputTypes());
        }

        @Override
        public Streamer<?>[] visitCollectNode(CollectPhase node, Void context) {
            return DataTypes.getStreamer(node.outputTypes());
        }

        @Override
        protected Streamer<?>[] visitExecutionPhase(ExecutionPhase node, Void context) {
            throw new UnsupportedOperationException(String.format("Got unsupported ExecutionNode %s", node.getClass().getName()));
        }
    }
}
