/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.fetch;

import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

public class FetchPhase implements ExecutionPhase {

    public final static ExecutionPhaseFactory<FetchPhase> FACTORY = new ExecutionPhaseFactory<FetchPhase>() {
        @Override
        public FetchPhase create() {
            return new FetchPhase();
        }
    };

    private TreeMap<String, Integer> bases;

    private UUID jobId;
    private int executionPhaseId;
    private Collection<Integer> collectPhaseIds;
    private Set<String> executionNodes;

    private FetchPhase() {}

    public FetchPhase(UUID jobId,
                      int executionPhaseId,
                      Collection<Integer> collectPhaseIds,
                      Set<String> executionNodes,
                      TreeMap<String, Integer> bases
                      ) {
        this.jobId = jobId;
        this.executionPhaseId = executionPhaseId;
        this.collectPhaseIds = collectPhaseIds;
        this.executionNodes = executionNodes;
        this.bases = bases;
    }

    @Override
    public Type type() {
        return Type.FETCH;
    }

    @Override
    public String name() {
        return "fetchPhase";
    }

    @Override
    public int executionPhaseId() {
        return executionPhaseId;
    }

    @Override
    public Set<String> executionNodes() {
        return executionNodes;
    }

    @Override
    public UUID jobId() {
        return jobId;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitFetchPhase(this, context);
    }

    public Collection<Integer> collectPhaseIds() {
        return collectPhaseIds;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        jobId = new UUID(in.readLong(), in.readLong());
        executionPhaseId = in.readVInt();

        int numExecutionNodes = in.readVInt();
        executionNodes = new HashSet<>(numExecutionNodes);
        for (int i = 0; i < numExecutionNodes; i++) {
            executionNodes.add(in.readString());
        }

        int numCollectPhaseIds = in.readVInt();
        collectPhaseIds = new HashSet<>(numCollectPhaseIds);
        for (int i = 0; i < numCollectPhaseIds; i++) {
            collectPhaseIds.add(in.readVInt());
        }
        int numBases = in.readVInt();
        bases = new TreeMap<>();
        for (int i = 0; i < numBases; i++) {
            bases.put(in.readString(), in.readVInt());
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(executionPhaseId);

        out.writeVInt(executionNodes.size());
        for (String executionNode : executionNodes) {
            out.writeString(executionNode);
        }

        out.writeVInt(collectPhaseIds.size());
        for (Integer collectPhaseId : collectPhaseIds) {
            out.writeVInt(collectPhaseId);
        }
        out.writeVInt(bases.size());
        for (Map.Entry<String, Integer> entry : bases.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue());
        }
    }

    public TreeMap<String, Integer> bases() {
        return bases;
    }
}
