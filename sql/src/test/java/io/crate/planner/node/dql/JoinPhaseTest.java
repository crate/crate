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

package io.crate.planner.node.dql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.TopNProjection;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;

public class JoinPhaseTest extends CrateUnitTest {

    private TopNProjection topNProjection;
    private UUID jobId;
    private MergePhase mp1;
    private MergePhase mp2;
    private Symbol joinCondition;

    @Before
    public void setup() {
        topNProjection = new TopNProjection(10, 0, Collections.emptyList());
        jobId = UUID.randomUUID();
        mp1 = new MergePhase(
            jobId,
            2,
            "merge",
            1,
            1,
            Collections.emptyList(),
            ImmutableList.<DataType>of(DataTypes.STRING),
            ImmutableList.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null);
        mp2 = new MergePhase(
            jobId,
            3,
            "merge",
            1,
            1,
            Collections.emptyList(),
            ImmutableList.<DataType>of(DataTypes.STRING),
            ImmutableList.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null);
        joinCondition = EqOperator.createFunction(
            new InputColumn(0, DataTypes.STRING), new InputColumn(1, DataTypes.STRING));
    }

    @Test
    public void testNestedLoopSerialization() throws Exception {
        NestedLoopPhase node = new NestedLoopPhase(
            jobId,
            1,
            "nestedLoop",
            ImmutableList.of(topNProjection),
            mp1,
            mp2,
            2,
            3,
            Sets.newHashSet("node1", "node2"),
            JoinType.FULL,
            joinCondition);

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
    }

    @Test
    public void testHashJoinSerialization() throws Exception {
        HashJoinPhase node = new HashJoinPhase(
            jobId,
            1,
            "nestedLoop",
            ImmutableList.of(topNProjection),
            mp1,
            mp2,
            2,
            3,
            Sets.newHashSet("node1", "node2"),
            joinCondition,
            Arrays.asList(Literal.of("testLeft"), Literal.of(10)),
            Arrays.asList(Literal.of("testRight"), Literal.of(20)),
            Arrays.asList(DataTypes.STRING, DataTypes.INTEGER),
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
