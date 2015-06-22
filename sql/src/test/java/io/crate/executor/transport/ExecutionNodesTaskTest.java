/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.Routing;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeGrouper;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ExecutionNodesTaskTest {


    @Test
    public void testGroupByServer() throws Exception {

        Routing twoNodeRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
                .put("node1", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(1, 2)).map())
                .put("node2", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(3, 4)).map())
                .map());

        UUID jobId = UUID.randomUUID();
        CollectNode c1 = new CollectNode(jobId, 1, "c1", twoNodeRouting);

        MergeNode m1 = new MergeNode(jobId, 2, "merge1", 2);
        m1.executionNodes(Sets.newHashSet("node3", "node4"));

        MergeNode m2 = new MergeNode(jobId, 3, "merge2", 2);

        m2.executionNodes(Sets.newHashSet("node1", "node3"));

        Map<String, Collection<ExecutionNode>> groupByServer = ExecutionNodeGrouper.groupByServer("node1", ImmutableList.<List<ExecutionNode>>of(ImmutableList.<ExecutionNode>of(c1, m1, m2)));

        assertThat(groupByServer.containsKey("node1"), is(true));
        assertThat(groupByServer.get("node1"), Matchers.<ExecutionNode>containsInAnyOrder(c1, m2));

        assertThat(groupByServer.containsKey("node2"), is(true));
        assertThat(groupByServer.get("node2"), Matchers.<ExecutionNode>containsInAnyOrder(c1));

        assertThat(groupByServer.containsKey("node3"), is(true));
        assertThat(groupByServer.get("node3"), Matchers.<ExecutionNode>containsInAnyOrder(m1, m2));

        assertThat(groupByServer.containsKey("node4"), is(true));
        assertThat(groupByServer.get("node4"), Matchers.<ExecutionNode>containsInAnyOrder(m1));
    }


    @Test
    public void testDetectsHasDirectResponse() throws Exception {
        CollectNode c1 = new CollectNode(UUID.randomUUID(), 1, "c1");
        c1.downstreamNodes(Collections.singletonList("foo"));

        assertThat(ExecutionNodesTask.hasDirectResponse(ImmutableList.<List<ExecutionNode>>of(ImmutableList.<ExecutionNode>of(c1))), is(false));

        CollectNode c2 = new CollectNode(UUID.randomUUID(), 1, "c1");
        c2.downstreamNodes(Collections.singletonList(ExecutionNode.DIRECT_RETURN_DOWNSTREAM_NODE));
        assertThat(ExecutionNodesTask.hasDirectResponse(ImmutableList.<List<ExecutionNode>>of(ImmutableList.<ExecutionNode>of(c2))), is(true));
    }
}