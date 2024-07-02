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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.sql.tree.JoinType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class HashJoinPhase extends JoinPhase {

    private final List<Symbol> leftJoinConditionInputs;
    private final List<Symbol> rightJoinConditionInputs;

    private final List<DataType<?>> leftOutputTypes;
    private final long estimatedRowSizeForLeft;

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
                         List<DataType<?>> leftOutputTypes,
                         long estimatedRowSizeForLeft) {
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
        this.leftJoinConditionInputs = leftJoinConditionInputs;
        this.rightJoinConditionInputs = rightJoinConditionInputs;
        this.leftOutputTypes = leftOutputTypes;
        this.estimatedRowSizeForLeft = estimatedRowSizeForLeft;
    }

    public HashJoinPhase(StreamInput in) throws IOException {
        super(in);

        leftJoinConditionInputs = Symbols.fromStream(in);
        rightJoinConditionInputs = Symbols.fromStream(in);
        leftOutputTypes = DataTypes.listFromStream(in);

        estimatedRowSizeForLeft = in.readZLong();
        if (in.getVersion().before(Version.V_5_6_0)) {
            // Version before 5.6.0 used to send numberOfRowsForLeft
            in.readZLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        Symbols.toStream(leftJoinConditionInputs, out);
        Symbols.toStream(rightJoinConditionInputs, out);
        DataTypes.toStream(leftOutputTypes, out);

        out.writeZLong(estimatedRowSizeForLeft);
        if (out.getVersion().before(Version.V_5_6_0)) {
            // Version before 5.6.0 used to send numberOfRowsForLeft,
            // sending neutral value, indicating that this stat is unavailable.
            out.writeZLong(-1);
        }
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

    public List<DataType<?>> leftOutputTypes() {
        return leftOutputTypes;
    }

    public long estimatedRowSizeForLeft() {
        return estimatedRowSizeForLeft;
    }
}
