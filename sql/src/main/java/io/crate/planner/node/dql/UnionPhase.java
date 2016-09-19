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

package io.crate.planner.node.dql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static io.crate.planner.distribution.DistributionInfo.DEFAULT_SAME_NODE;

public class UnionPhase extends AbstractProjectionsPhase implements UpstreamPhase {

    public static final ExecutionPhaseFactory<UnionPhase> FACTORY = new ExecutionPhaseFactory<UnionPhase>() {
        @Override
        public UnionPhase create() {
            return new UnionPhase();
        }
    };

    private Collection<String> executionNodes;
    private Collection<MergePhase> mergePhases;

    private UnionPhase() {
    }

    public UnionPhase(UUID jobId,
                      int executionNodeId,
                      String name,
                      List<Projection> projections,
                      Collection<MergePhase> mergePhases,
                      List<Symbol> outputs,
                      Collection<String> executionNodes) {
        super(jobId, executionNodeId, name, projections);
        assert !mergePhases.isEmpty();
        if (projections.isEmpty()) {
            outputTypes = Symbols.extractTypes(outputs);
        } else {
            outputTypes = Symbols.extractTypes(Iterables.getLast(projections).outputs());
        }
        this.mergePhases = mergePhases;
        this.executionNodes = executionNodes;
    }

    @Override
    public Type type() {
        return Type.UNION;
    }

    @Override
    public Collection<String> executionNodes() {
        return executionNodes;
    }

    public Collection<MergePhase> mergePhases() {
        return mergePhases;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitUnionPhase(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("UnionPhase cannot be streamed");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("UnionPhase cannot be streamed");
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
            .add("executionPhaseId", executionPhaseId())
            .add("name", name())
            .add("outputTypes", outputTypes)
            .add("jobId", jobId())
            .add("executionNodes", executionNodes)
            .add("mergePhases", mergePhases);
        return helper.toString();
    }

    @Override
    public DistributionInfo distributionInfo() {
        return DEFAULT_SAME_NODE;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        throw new UnsupportedOperationException("UnionPhase is not distributable");
    }
}
