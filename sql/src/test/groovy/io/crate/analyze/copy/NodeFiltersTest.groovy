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

package io.crate.analyze.copy

import io.crate.test.integration.CrateUnitTest
import org.junit.Test

import static io.crate.testing.DiscoveryNodes.newNode

class NodeFiltersTest extends CrateUnitTest {

    @Test
    public void testIdFilter() throws Exception {
        NodeFilters filters = new NodeFilters(null, "n[1-3]")

        assert filters.test(newNode("n1", "n1"))
        assert filters.test(newNode("n2", "n2"))
        assert filters.test(newNode("n3", "n3"))
        assert !filters.test(newNode("n4", "n4"))
    }

    @Test
    public void testNodeNameFilter() throws Exception {
        NodeFilters filters = new NodeFilters("node[1-3]", null)
        assert filters.test(newNode("node1", "n2"))
        assert !filters.test(newNode("node4", "n1"))
    }

    @Test
    public void testNameAndIdFilter() throws Exception {
        NodeFilters filters = new NodeFilters("node[1-3]", "n[1-2]")

        assert filters.test(newNode("node1", "n1"))
        assert !filters.test(newNode("node1", "n4"))
        assert !filters.test(newNode("node4", "n2"))
    }
}
