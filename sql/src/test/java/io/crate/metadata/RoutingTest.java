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

package io.crate.metadata;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.is;

public class RoutingTest extends CrateUnitTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testStreamingWithLocations() throws Exception {
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        Map<String, List<Integer>> indexMap = new TreeMap<>();
        indexMap.put("index-0", Arrays.asList(1, 2));
        locations.put("node-0", indexMap);

        BytesStreamOutput out = new BytesStreamOutput();
        Routing routing1 = new Routing(locations);
        routing1.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        Routing routing2 = new Routing();
        routing2.readFrom(in);

        assertThat(routing1.locations(), is(routing2.locations()));
        assertThat(routing1.jobSearchContextIdBase(), is(routing2.jobSearchContextIdBase()));
    }

    @Test
    public void testStreamingWithoutLocations() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        Routing routing1 = new Routing(null);
        routing1.jobSearchContextIdBase(10);
        routing1.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        Routing routing2 = new Routing();
        routing2.readFrom(in);

        assertThat(routing1.locations(), is(routing2.locations()));
        assertThat(routing1.jobSearchContextIdBase(), is(routing2.jobSearchContextIdBase()));
    }
}
