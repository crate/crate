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
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;

public class NestedLoopNodeTest extends CrateUnitTest {

    @Test
    public void testSerialization() throws Exception {

        NestedLoopNode node = new NestedLoopNode(1, "nestedLoop");
        node.jobId(UUID.randomUUID());
        node.executionNodes(Sets.newHashSet("node1", "node2"));
        node.leftInputTypes(Arrays.<DataType>asList(DataTypes.UNDEFINED, DataTypes.STRING));
        node.rightInputTypes(Arrays.<DataType>asList(DataTypes.BOOLEAN, DataTypes.INTEGER, DataTypes.DOUBLE));
        node.downstreamNodes(Sets.newHashSet("node3", "node4"));
        node.downstreamExecutionNodeId(5);
        TopNProjection topNProjection = new TopNProjection(10, 0);
        node.projections(ImmutableList.<Projection>of(topNProjection));

        BytesStreamOutput output = new BytesStreamOutput();
        node.writeTo(output);

        BytesStreamInput input = new BytesStreamInput(output.bytes());
        NestedLoopNode node2 = new NestedLoopNode();
        node2.readFrom(input);

        assertThat(node.downstreamExecutionNodeId(), is(node2.downstreamExecutionNodeId()));
        assertThat(node.downstreamNodes(), is(node2.downstreamNodes()));
        assertThat(node.executionNodes(), Is.is(node2.executionNodes()));
        assertThat(node.jobId(), Is.is(node2.jobId()));
        assertThat(node.leftInputTypes(), is(node2.leftInputTypes()));
        assertThat(node.rightInputTypes(), is(node2.rightInputTypes()));
        assertThat(node.executionNodeId(), is(node2.executionNodeId()));
        assertThat(node.name(), is(node2.name()));
        assertThat(node.outputTypes(), is(node2.outputTypes()));
    }
}
