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

package io.crate.execution.dsl.phases;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class NodeOperation implements Writeable {

    private static final Logger LOGGER = LogManager.getLogger(NodeOperation.class);

    public static final int NO_DOWNSTREAM = Integer.MAX_VALUE;

    private ExecutionPhase executionPhase;
    private Collection<String> downstreamNodes;
    private int downstreamExecutionPhaseId;
    private byte downstreamExecutionPhaseInputId;

    public NodeOperation(ExecutionPhase executionPhase,
                         Collection<String> downstreamNodes,
                         int downstreamExecutionPhaseId,
                         byte downstreamExecutionPhaseInputId) {
        this.executionPhase = executionPhase;
        this.downstreamNodes = downstreamNodes;
        this.downstreamExecutionPhaseId = downstreamExecutionPhaseId;
        this.downstreamExecutionPhaseInputId = downstreamExecutionPhaseInputId;
    }

    public NodeOperation(StreamInput in) throws IOException {
        executionPhase = ExecutionPhases.fromStream(in);
        downstreamExecutionPhaseId = in.readVInt();
        downstreamExecutionPhaseInputId = in.readByte();
        int numExecutionNodes = in.readVInt();

        List<String> executionNodes = new ArrayList<>();
        for (int i = 0; i < numExecutionNodes; i++) {
            executionNodes.add(in.readString());
        }
        this.downstreamNodes = executionNodes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExecutionPhases.toStream(out, executionPhase);
        out.writeVInt(downstreamExecutionPhaseId);
        out.writeByte(downstreamExecutionPhaseInputId);

        out.writeVInt(downstreamNodes.size());
        for (String executionNode : downstreamNodes) {
            out.writeString(executionNode);
        }
    }

    public static NodeOperation withoutDownstream(ExecutionPhase executionPhase) {
        return new NodeOperation(executionPhase,
            List.of(),
            ExecutionPhase.NO_EXECUTION_PHASE,
            (byte) 0);
    }

    /**
     * Create a NodeOperation for upstreamPhase with downstreamPhase as the target/downstream.
     * The downstreamPhase will receive the data from upstreamPhase via directResponse unless
     * a SAME_NODE execution is possible
     */
    public static NodeOperation withDirectResponse(ExecutionPhase upstreamPhase,
                                                   ExecutionPhase downstreamPhase,
                                                   byte inputId,
                                                   String localNodeId) {
        Collection<String> downstreamNodes;
        Collection<String> upstreamNodeIds = upstreamPhase.nodeIds();
        if (upstreamNodeIds.size() == 1 && downstreamPhase.nodeIds().size() == 1 &&
            downstreamPhase.nodeIds().contains(localNodeId) &&
            upstreamNodeIds.contains(localNodeId)) {
            LOGGER.trace("Phase uses SAME_NODE downstream, reason: ON HANDLER, executionNodes: {}, phase: {}",
                upstreamPhase.nodeIds(), upstreamPhase);
            downstreamNodes = Collections.emptyList();
        } else {
            downstreamNodes = ExecutionPhase.DIRECT_RESPONSE_LIST;
        }
        return new NodeOperation(
            upstreamPhase,
            downstreamNodes,
            downstreamPhase.phaseId(),
            inputId
        );
    }

    /**
     * Create a NodeOperation for upstreamPhase with downstreamPhase as the target/downstream.
     * This will result in a push-based execution.
     *
     * See also: {@link #withDirectResponse(ExecutionPhase, ExecutionPhase, byte, String)}
     */
    public static NodeOperation withDownstream(ExecutionPhase upstreamPhase, ExecutionPhase downstreamPhase, byte inputId) {
        return new NodeOperation(
            upstreamPhase,
            downstreamPhase.nodeIds(),
            downstreamPhase.phaseId(),
            inputId);
    }

    public ExecutionPhase executionPhase() {
        return executionPhase;
    }

    public Collection<String> downstreamNodes() {
        return downstreamNodes;
    }

    public int downstreamExecutionPhaseId() {
        return downstreamExecutionPhaseId;
    }

    @Override
    public String toString() {
        return "NodeOp{ " + ExecutionPhases.debugPrint(executionPhase) +
               ", downstreamNodes=" + downstreamNodes +
               ", downstreamPhase=" + downstreamExecutionPhaseId +
               ", downstreamInputId=" + downstreamExecutionPhaseInputId +
               '}';
    }

    public byte downstreamExecutionPhaseInputId() {
        return downstreamExecutionPhaseInputId;
    }

}
