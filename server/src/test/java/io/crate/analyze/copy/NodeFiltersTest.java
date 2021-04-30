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

package io.crate.analyze.copy;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static io.crate.testing.DiscoveryNodes.newNode;
import static org.hamcrest.core.Is.is;

public class NodeFiltersTest extends ESTestCase {

    @Test
    public void testIdFilter() {
        NodeFilters filters = new NodeFilters(null, "n[1-3]");

        assertThat(filters.test(newNode("n1", "n1")), is(true));
        assertThat(filters.test(newNode("n2", "n2")), is(true));
        assertThat(filters.test(newNode("n3", "n3")), is(true));
        assertThat(filters.test(newNode("n4", "n4")), is(false));
    }

    @Test
    public void testNodeNameFilter() {
        NodeFilters filters = new NodeFilters("node[1-3]", null);
        assertThat(filters.test(newNode("node1", "n2")), is(true));
        assertThat(filters.test(newNode("node4", "n1")), is(false));
    }

    @Test
    public void testNameAndIdFilter() {
        NodeFilters filters = new NodeFilters("node[1-3]", "n[1-2]");

        assertThat(filters.test(newNode("node1", "n1")), is(true));
        assertThat(filters.test(newNode("node1", "n4")), is(false));
        assertThat(filters.test(newNode("node4", "n2")), is(false));
    }
}
