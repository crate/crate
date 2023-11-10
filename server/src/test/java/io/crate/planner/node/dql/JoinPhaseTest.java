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

package io.crate.planner.node.dql;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.sql.tree.JoinType;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class JoinPhaseTest extends ESTestCase {

    private LimitAndOffsetProjection limitAndOffsetProjection;
    private UUID jobId;
    private MergePhase mp1;
    private MergePhase mp2;
    private Symbol joinCondition;

    @Before
    public void setup() {
        limitAndOffsetProjection = new LimitAndOffsetProjection(10, 0, Collections.emptyList());
        jobId = UUID.randomUUID();
        mp1 = new MergePhase(
            jobId,
            2,
            "merge",
            1,
            1,
            Collections.emptyList(),
            List.of(DataTypes.STRING),
            List.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null);
        mp2 = new MergePhase(
            jobId,
            3,
            "merge",
            1,
            1,
            Collections.emptyList(),
            List.of(DataTypes.STRING),
            List.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null);
        joinCondition = new Function(
            EqOperator.SIGNATURE,
            List.of(
                new InputColumn(0, DataTypes.STRING),
                new InputColumn(1, DataTypes.STRING)
            ),
            EqOperator.RETURN_TYPE
        );
    }

    @Test
    public void testNestedLoopSerialization() throws Exception {
        NestedLoopPhase node = new NestedLoopPhase(
            jobId,
            1,
            "nestedLoop",
            List.of(limitAndOffsetProjection),
            mp1,
            mp2,
            2,
            3,
            Set.of("node1", "node2"),
            JoinType.FULL,
            joinCondition,
            List.of(DataTypes.LONG, DataTypes.STRING, new ArrayType<>(DataTypes.INTEGER)),
            32L,
            100_000,
            true);

        BytesStreamOutput output = new BytesStreamOutput();
        node.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        NestedLoopPhase node2 = new NestedLoopPhase(input);

        assertThat(node.nodeIds(), is(node2.nodeIds()));
        assertThat(node.jobId(), is(node2.jobId()));
        assertThat(node.joinCondition(), is(node2.joinCondition()));
        assertThat(node.type(), is(node2.type()));
        assertThat(node.nodeIds(), is(node2.nodeIds()));
        assertThat(node.jobId(), is(node2.jobId()));
        assertThat(node.name(), is(node2.name()));
        assertThat(node.outputTypes(), is(node2.outputTypes()));
        assertThat(node.joinType(), is(node2.joinType()));
        assertThat(node.joinCondition(), is(node2.joinCondition()));
        assertThat(node.estimatedRowsSizeLeft, is(32L));
        assertThat(node.estimatedNumberOfRowsLeft, is(100_000L));
        assertThat(node.blockNestedLoop, is(true));
    }

    @Test
    public void testHashJoinSerialization() throws Exception {
        HashJoinPhase node = new HashJoinPhase(
            jobId,
            1,
            "nestedLoop",
            List.of(limitAndOffsetProjection),
            mp1,
            mp2,
            2,
            3,
            Set.of("node1", "node2"),
            joinCondition,
            List.of(Literal.of("testLeft"), Literal.of(10)),
            List.of(Literal.of("testRight"), Literal.of(20)),
            List.of(DataTypes.STRING, DataTypes.INTEGER),
            111);

        BytesStreamOutput output = new BytesStreamOutput();
        node.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        HashJoinPhase node2 = new HashJoinPhase(input);

        assertThat(node.nodeIds(), is(node2.nodeIds()));
        assertThat(node.jobId(), is(node2.jobId()));
        assertThat(node.joinCondition(), is(node2.joinCondition()));
        assertThat(node.type(), is(node2.type()));
        assertThat(node.nodeIds(), is(node2.nodeIds()));
        assertThat(node.jobId(), is(node2.jobId()));
        assertThat(node.name(), is(node2.name()));
        assertThat(node.outputTypes(), is(node2.outputTypes()));
        assertThat(node.joinType(), is(node2.joinType()));
        assertThat(node.joinCondition(), is(node2.joinCondition()));
        assertThat(node.leftJoinConditionInputs(), is(node2.leftJoinConditionInputs()));
        assertThat(node.rightJoinConditionInputs(), is(node2.rightJoinConditionInputs()));
        assertThat(node.numLeftOutputs(), is(node2.numLeftOutputs()));
        assertThat(node.numRightOutputs(), is(node2.numRightOutputs()));
        assertThat(node.leftOutputTypes(), is(node2.leftOutputTypes()));
        assertThat(node.estimatedRowSizeForLeft(), is(node2.estimatedRowSizeForLeft()));
    }
}
