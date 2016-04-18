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

package io.crate.planner.node;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.core.Is.is;

public class MergeNodeTest extends CrateUnitTest {


    @Test
    public void testSerialization() throws Exception {

        Reference nameRef = TestingHelpers.createReference("name", DataTypes.STRING);
        GroupProjection groupProjection = new GroupProjection();
        groupProjection.keys(Collections.<Symbol>singletonList(nameRef));
        groupProjection.values(Collections.singletonList(
                Aggregation.finalAggregation(
                        new FunctionInfo(new FunctionIdent(CountAggregation.NAME, ImmutableList.<DataType>of()), DataTypes.LONG),
                        ImmutableList.<Symbol>of(),
                        Aggregation.Step.PARTIAL)
        ));
        TopNProjection topNProjection = new TopNProjection(10, 0);

        List<Projection> projections = Arrays.asList(groupProjection, topNProjection);
        MergePhase node = new MergePhase(
                UUID.randomUUID(), 0, "merge", 2,
                Arrays.<DataType>asList(DataTypes.UNDEFINED, DataTypes.STRING),
                projections,
                DistributionInfo.DEFAULT_BROADCAST);
        node.executionNodes(Sets.newHashSet("node1", "node2"));

        BytesStreamOutput output = new BytesStreamOutput();
        node.writeTo(output);


        StreamInput input = StreamInput.wrap(output.bytes());
        MergePhase node2 = MergePhase.FACTORY.create();
        node2.readFrom(input);

        assertThat(node.numUpstreams(), is(node2.numUpstreams()));
        assertThat(node.executionNodes(), is(node2.executionNodes()));
        assertThat(node.jobId(), is(node2.jobId()));
        assertEquals(node.inputTypes(), node2.inputTypes());
        assertThat(node.executionPhaseId(), is(node2.executionPhaseId()));
        assertThat(node.distributionInfo(), is(node2.distributionInfo()));
    }
}
