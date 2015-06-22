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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.hamcrest.core.Is.is;

public class MergeNodeTest extends CrateUnitTest {


    @Test
    public void testSerialization() throws Exception {
        MergeNode node = new MergeNode(UUID.randomUUID(), 0, "merge", 2);
        node.executionNodes(Sets.newHashSet("node1", "node2"));
        node.inputTypes(Arrays.<DataType>asList(DataTypes.UNDEFINED, DataTypes.STRING));
        node.downstreamNodes(Sets.newHashSet("node3", "node4"));

        Reference nameRef = TestingHelpers.createReference("name", DataTypes.STRING);
        GroupProjection groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(nameRef));
        groupProjection.values(Arrays.asList(
                new Aggregation(
                        new FunctionInfo(new FunctionIdent(CountAggregation.NAME, ImmutableList.<DataType>of()), DataTypes.LONG),
                        ImmutableList.<Symbol>of(),
                        Aggregation.Step.PARTIAL,
                        Aggregation.Step.FINAL
                )
        ));
        TopNProjection topNProjection = new TopNProjection(10, 0);

        node.projections(Arrays.asList(groupProjection, topNProjection));

        BytesStreamOutput output = new BytesStreamOutput();
        node.writeTo(output);


        BytesStreamInput input = new BytesStreamInput(output.bytes());
        MergeNode node2 = new MergeNode();
        node2.readFrom(input);

        assertThat(node.downstreamNodes(), is(node2.downstreamNodes()));
        assertThat(node.numUpstreams(), is(node2.numUpstreams()));
        assertThat(node.executionNodes(), is(node2.executionNodes()));
        assertThat(node.jobId(), is(node2.jobId()));
        assertThat(node.inputTypes(), is(node2.inputTypes()));
        assertThat(node.executionNodeId(), is(node2.executionNodeId()));
    }
}
