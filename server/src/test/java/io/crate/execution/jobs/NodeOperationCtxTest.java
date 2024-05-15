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

package io.crate.execution.jobs;

import static io.crate.planner.distribution.DistributionInfo.DEFAULT_BROADCAST;
import static io.crate.testing.StubPhases.newPhase;
import static java.util.Collections.singletonList;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.jobs.JobSetup.NodeOperationCtx;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.testing.StubPhases;

public class NodeOperationCtxTest extends ESTestCase {

    @Test
    public void testFindLeafs() {
        ExecutionPhase p1 = newPhase(0, "n1", "n2");
        ExecutionPhase p2 = newPhase(1, "n1", "n2");
        ExecutionPhase p3 = newPhase(2, "n2");

        NodeOperationCtx opCtx = new NodeOperationCtx(
            "n1",
            List.of(
                NodeOperation.withDownstream(p1, p2, (byte) 0),
                NodeOperation.withDownstream(p2, p3, (byte) 0))
        );

        assertThat(opCtx.findLeafs(), is(IntArrayList.from(2)));
    }

    @Test
    public void testFindLeafWithNodeOperationsThatHaveNoLeaf() {
        ExecutionPhase p1 = newPhase(0, "n1");
        NodeOperationCtx opCtx =
            new NodeOperationCtx("n1", singletonList(NodeOperation.withDownstream(p1, p1, (byte) 0)));

        assertThat(stream(opCtx.findLeafs().spliterator(), false).count(), is(0L));
    }

    @Test
    public void testIsUpstreamOnSameNodeWithSameNodeOptimization() {
        ExecutionPhase p1 = StubPhases.newUpstreamPhase(0, DEFAULT_BROADCAST, "n1");
        ExecutionPhase p2 = newPhase(1, "n1");
        NodeOperationCtx opCtx = new NodeOperationCtx("n1", singletonList(NodeOperation.withDownstream(p1, p2, (byte) 0)));

        // withDownstream set DistributionInfo to SAME_NODE because both phases are on n1
        assertThat(opCtx.upstreamsAreOnSameNode(1)).isTrue();
    }

    @Test
    public void testIsUpstreamOnSameNodeWithUpstreamOnOtherNode() {
        ExecutionPhase p1 = StubPhases.newUpstreamPhase(0, DEFAULT_BROADCAST, "n2");
        ExecutionPhase p2 = newPhase(1, "n1");
        NodeOperationCtx opCtx = new NodeOperationCtx("n1", singletonList(NodeOperation.withDownstream(p1, p2, (byte) 0)));

        assertThat(opCtx.upstreamsAreOnSameNode(1)).isFalse();
    }

    @Test
    public void testIsUpstreamOnSameNodeWithTwoUpstreamsThatAreOnTheSameNode() {
        ExecutionPhase p1 = StubPhases.newUpstreamPhase(0, DistributionInfo.DEFAULT_SAME_NODE, "n2");
        ExecutionPhase p2 = StubPhases.newUpstreamPhase(2, DistributionInfo.DEFAULT_SAME_NODE, "n2");
        ExecutionPhase p3 = newPhase(3, "n2");

        NodeOperationCtx opCtx = new NodeOperationCtx("n1",
            List.of(NodeOperation.withDownstream(p1, p3, (byte) 0), NodeOperation.withDownstream(p2, p3, (byte) 0)));

        assertThat(opCtx.upstreamsAreOnSameNode(3)).isTrue();
    }
}
