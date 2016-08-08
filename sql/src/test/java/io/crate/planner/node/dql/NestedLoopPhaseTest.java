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
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;

public class NestedLoopPhaseTest extends CrateUnitTest {

    @Test
    public void testSerialization() throws Exception {
        TopNProjection topNProjection = new TopNProjection(10, 0);
        UUID jobId = UUID.randomUUID();
        MergePhase mp1 = new MergePhase(jobId, 2, "merge", 1,
                ImmutableList.<DataType>of(DataTypes.STRING),
                ImmutableList.<Projection>of(),
                DistributionInfo.DEFAULT_BROADCAST);
        MergePhase mp2 = new MergePhase(jobId, 3, "merge", 1,
                ImmutableList.<DataType>of(DataTypes.STRING),
                ImmutableList.<Projection>of(),
                DistributionInfo.DEFAULT_BROADCAST);
        SqlExpressions sqlExpressions = new SqlExpressions(T3.SOURCES, T3.TR_1);
        Symbol filterCondition = sqlExpressions.normalize(sqlExpressions.asSymbol("a = 'foo'"));
        NestedLoopPhase node = new NestedLoopPhase(
            jobId,
            1,
            "nestedLoop",
            ImmutableList.<Projection>of(topNProjection),
            mp1,
            mp2,
            Sets.newHashSet("node1", "node2"),
            JoinType.INNER,
            filterCondition
            );

        BytesStreamOutput output = new BytesStreamOutput();
        node.writeTo(output);

        StreamInput input = StreamInput.wrap(output.bytes());
        NestedLoopPhase node2 = new NestedLoopPhase();
        node2.readFrom(input);

        assertThat(node.executionNodes(), Is.is(node2.executionNodes()));
        assertThat(node.jobId(), Is.is(node2.jobId()));
        assertThat(node.name(), is(node2.name()));
        assertThat(node.outputTypes(), is(node2.outputTypes()));
        assertThat(node.filterSymbol(), is(node2.filterSymbol()));
        assertThat(node.joinType(), is(node2.joinType()));
    }
}
