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

package io.crate.action.job

import com.carrotsearch.hppc.IntArrayList
import io.crate.execution.dsl.phases.NodeOperation
import io.crate.execution.jobs.ContextPreparer
import io.crate.planner.distribution.DistributionInfo
import io.crate.testing.StubPhases
import org.junit.Test

class NodeOperationCtxTest {

    @Test
    void testFindLeafs() {
        def p1 = StubPhases.newPhase(0, "n1", "n2")
        def p2 = StubPhases.newPhase(1, "n1", "n2")
        def p3 = StubPhases.newPhase(2, "n2")

        def opCtx = new ContextPreparer.NodeOperationCtx("n1", [
                NodeOperation.withDownstream(p1, p2, (byte)0),
                NodeOperation.withDownstream(p2, p3, (byte)0)])

        def expected = new IntArrayList()
        expected.add(2)

        assert opCtx.findLeafs() == expected
    }

    @Test
    void testFindLeafWithNodeOperationsThatHaveNoLeaf() {
        def p1 = StubPhases.newPhase(0, "n1");
        def opCtx = new ContextPreparer.NodeOperationCtx("n1", [NodeOperation.withDownstream(p1, p1, (byte)0)])

        opCtx.findLeafs().size() == 0;
    }

    @Test
    void testIsUpstreamOnSameNodeWithSameNodeOptimization() throws Exception {
        def p1 = StubPhases.newUpstreamPhase(0, DistributionInfo.DEFAULT_BROADCAST, "n1")
        def p2 = StubPhases.newPhase(1, "n1")
        def opCtx = new ContextPreparer.NodeOperationCtx("n1", [NodeOperation.withDownstream(p1, p2, (byte)0)])

        // withDownstream set DistributionInfo to SAME_NODE because both phases are on n1
        assert opCtx.upstreamsAreOnSameNode(1)
    }

    @Test
    void testIsUpstreamOnSameNodeWithUpstreamOnOtherNode() throws Exception {
        def p1 = StubPhases.newUpstreamPhase(0, DistributionInfo.DEFAULT_BROADCAST, "n2")
        def p2 = StubPhases.newPhase(1, "n1")
        def opCtx = new ContextPreparer.NodeOperationCtx("n1", [NodeOperation.withDownstream(p1, p2, (byte)0)])

        assert !opCtx.upstreamsAreOnSameNode(1)
    }

    @Test
    void testIsUpstreamOnSameNodeWithTwoUpstreamsThatAreOnTheSameNode() throws Exception {
        def p1 = StubPhases.newUpstreamPhase(0, DistributionInfo.DEFAULT_SAME_NODE, "n2")
        def p2 = StubPhases.newUpstreamPhase(2, DistributionInfo.DEFAULT_SAME_NODE, "n2")
        def p3 = StubPhases.newPhase(3, "n2")

        def opCtx = new ContextPreparer.NodeOperationCtx("n1",[
                NodeOperation.withDownstream(p1, p3, (byte)0),
                NodeOperation.withDownstream(p2, p3, (byte)0)])


        assert opCtx.upstreamsAreOnSameNode(3)
    }
}
