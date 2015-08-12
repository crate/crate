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

package io.crate.operation;

import com.google.common.collect.ImmutableList;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhases;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NodeOperation implements Streamable {

    private ExecutionPhase executionPhase;
    private Collection<String> downstreamNodes;
    private int downstreamExecutionPhaseId;
    private byte downstreamExecutionPhaseInputId;

    private NodeOperation(ExecutionPhase executionPhase,
                          Collection<String> downstreamNodes,
                          int downstreamExecutionPhaseId,
                          byte downstreamExecutionPhaseInputId) {
        this.executionPhase = executionPhase;
        this.downstreamNodes = downstreamNodes;
        this.downstreamExecutionPhaseId = downstreamExecutionPhaseId;
        this.downstreamExecutionPhaseInputId = downstreamExecutionPhaseInputId;
    }

    public NodeOperation(StreamInput in) throws IOException {
        readFrom(in);
    }

    public static NodeOperation withoutDownstream(ExecutionPhase executionPhase) {
        return new NodeOperation(executionPhase,
                ImmutableList.<String>of(),
                ExecutionPhase.NO_EXECUTION_PHASE,
                (byte) 0);
    }

    public static NodeOperation withDownstream(ExecutionPhase executionPhase, ExecutionPhase downstreamExecutionPhase, byte inputId) {
        if (downstreamExecutionPhase.executionNodes().isEmpty()) {
            return new NodeOperation(
                    executionPhase,
                    ImmutableList.of("_response"),
                    downstreamExecutionPhase.executionPhaseId(),
                    inputId);
        } else {
            return new NodeOperation(executionPhase,
                    downstreamExecutionPhase.executionNodes(),
                    downstreamExecutionPhase.executionPhaseId(),
                    inputId);
        }
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
    public void readFrom(StreamInput in) throws IOException {
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

    @Override
    public String toString() {
        return "NodeOp{ " + executionPhase.executionPhaseId() + " " + executionPhase.name() +
                ", downstreamNodes=" + downstreamNodes +
                ", downstreamPhase=" + downstreamExecutionPhaseId +
                ", downstreamInputId=" + downstreamExecutionPhaseInputId +
                '}';
    }

    public byte downstreamExecutionPhaseInputId() {
        return downstreamExecutionPhaseInputId;
    }

}
