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

package io.crate.planner;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class DecommissionNodePlanTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testResolveIdUsingNodeId() throws Exception {
        assertThat(NodeSelection.resolveNodeId(clusterService.state().nodes(), NODE_ID),
                   is(NODE_ID)
        );
    }

    @Test
    public void testResolveIdUsingNodeName() throws Exception {
        assertThat(NodeSelection.resolveNodeId(clusterService.state().nodes(), NODE_NAME),
                   is(NODE_ID)
        );
    }

    @Test
    public void testResolveIdUsingNodeNameDiffCase() throws Exception {
        assertThat(NodeSelection.resolveNodeId(clusterService.state().nodes(), NODE_NAME.toUpperCase()),
                   is(NODE_ID)
        );
    }

    @Test
    public void testFailIfNodeIsNotPartOfTheCluster() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("could not be found");
        NodeSelection.resolveNodeId(clusterService.state().nodes(), "aNonExistentNode");
    }
}
