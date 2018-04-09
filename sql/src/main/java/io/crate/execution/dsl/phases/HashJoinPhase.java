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

package io.crate.execution.dsl.phases;

import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class HashJoinPhase extends JoinPhase {

    private final List<Symbol> leftJoinConditionInputs;
    private final List<Symbol> rightJoinConditionInputs;

    private final Collection<DataType> leftOutputTypes;
    private final long estimatedRowSizeForLeft;
    private final long numberOfRowsForLeft;
    private final int rowsToBeConsumed;

    public HashJoinPhase(UUID jobId,
                         int executionNodeId,
                         String name,
                         List<Projection> projections,
                         @Nullable MergePhase leftMergePhase,
                         @Nullable MergePhase rightMergePhase,
                         int numLeftOutputs,
                         int numRightOutputs,
                         Collection<String> executionNodes,
                         @Nullable Symbol joinCondition,
                         List<Symbol> leftJoinConditionInputs,
                         List<Symbol> rightJoinConditionInputs,
                         Collection<DataType> leftOutputTypes,
                         long estimatedRowSizeForLeft,
                         long numberOfRowsForLeft,
                         int rowsToBeConsumed) {
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
            JoinType.INNER,
            joinCondition);
        assert joinCondition != null : "JoinCondition for HashJoin cannot be null";
        assert rowsToBeConsumed >= 0 : "RowsToBeConsumed for HashJoin cannot be negative";
        this.leftJoinConditionInputs = leftJoinConditionInputs;
        this.rightJoinConditionInputs = rightJoinConditionInputs;
        this.leftOutputTypes = leftOutputTypes;
        this.estimatedRowSizeForLeft = estimatedRowSizeForLeft;
        this.numberOfRowsForLeft = numberOfRowsForLeft;
        this.rowsToBeConsumed = rowsToBeConsumed;
    }

    public HashJoinPhase(StreamInput in) throws IOException {
        super(in);

        leftJoinConditionInputs = Symbols.listFromStream(in);
        rightJoinConditionInputs = Symbols.listFromStream(in);
        leftOutputTypes = DataTypes.listFromStream(in);

        estimatedRowSizeForLeft = in.readLong();
        numberOfRowsForLeft = in.readLong();
        rowsToBeConsumed = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        Symbols.toStream(leftJoinConditionInputs, out);
        Symbols.toStream(rightJoinConditionInputs, out);
        DataTypes.toStream(leftOutputTypes, out);

        out.writeLong(estimatedRowSizeForLeft);
        out.writeLong(numberOfRowsForLeft);
        out.writeVInt(rowsToBeConsumed);
    }

    @Override
    public Type type() {
        return Type.HASH_JOIN;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitHashJoinPhase(this, context);
    }

    public List<Symbol> leftJoinConditionInputs() {
        return leftJoinConditionInputs;
    }

    public List<Symbol> rightJoinConditionInputs() {
        return rightJoinConditionInputs;
    }

    public Collection<DataType> leftOutputTypes() {
        return leftOutputTypes;
    }

    public long estimatedRowSizeForLeft() {
        return estimatedRowSizeForLeft;
    }

    public long numberOfRowsForLeft() {
        return numberOfRowsForLeft;
    }

    public int rowsToBeConsumed() {
        return rowsToBeConsumed;
    }
}
