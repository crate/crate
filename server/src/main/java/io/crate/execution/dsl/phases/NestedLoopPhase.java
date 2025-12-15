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

import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.JoinType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class NestedLoopPhase extends JoinPhase {

    public final List<DataType<?>> leftSideColumnTypes;
    public final long estimatedRowsSizeLeft;
    public final long estimatedNumberOfRowsLeft;
    public final boolean blockNestedLoop;

    public NestedLoopPhase(UUID jobId,
                           int executionNodeId,
                           String name,
                           List<Projection> projections,
                           @Nullable MergePhase leftMergePhase,
                           @Nullable MergePhase rightMergePhase,
                           int numLeftOutputs,
                           int numRightOutputs,
                           Collection<String> executionNodes,
                           JoinType joinType,
                           @Nullable Symbol joinCondition,
                           List<DataType<?>> leftSideColumnTypes,
                           long estimatedRowsSizeLeft,
                           long estimatedNumberOfRowsLeft,
                           boolean blockNestedLoop) {
        super(
            jobId,
            executionNodeId,
            name,
            projections,
            leftMergePhase,
            rightMergePhase,
            numLeftOutputs,
            numRightOutputs,
            executionNodes,
            joinType,
            joinCondition);

        this.leftSideColumnTypes = leftSideColumnTypes;
        this.estimatedRowsSizeLeft = estimatedRowsSizeLeft;
        this.estimatedNumberOfRowsLeft = estimatedNumberOfRowsLeft;
        this.blockNestedLoop = blockNestedLoop;
    }

    public NestedLoopPhase(StreamInput in) throws IOException {
        super(in);
        this.leftSideColumnTypes = DataTypes.listFromStream(in);
        this.estimatedRowsSizeLeft = in.readZLong();
        this.estimatedNumberOfRowsLeft = in.readZLong();
        this.blockNestedLoop = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        DataTypes.toStream(leftSideColumnTypes, out);
        out.writeZLong(estimatedRowsSizeLeft);
        out.writeZLong(estimatedNumberOfRowsLeft);
        out.writeBoolean(blockNestedLoop);
    }

    @Override
    public Type type() {
        return Type.NESTED_LOOP;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopPhase(this, context);
    }
}
