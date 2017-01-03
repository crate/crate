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

package io.crate.planner.node.dql;

import io.crate.analyze.WhereClause;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.Routing;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class CountPhaseTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        Routing routing = new Routing(
            TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
                .put("n1", TreeMapBuilder.<String, List<Integer>>newMapBuilder()
                    .put("i1", Arrays.asList(1, 2))
                    .put("i2", Arrays.asList(1, 2)).map())
                .put("n2", TreeMapBuilder.<String, List<Integer>>newMapBuilder()
                    .put("i1", Collections.singletonList(3)).map()).map());
        CountPhase countPhase = new CountPhase(1, routing, WhereClause.MATCH_ALL, DistributionInfo.DEFAULT_BROADCAST);

        BytesStreamOutput out = new BytesStreamOutput(10);
        countPhase.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());

        CountPhase streamedNode = CountPhase.FACTORY.create();
        streamedNode.readFrom(in);

        assertThat(streamedNode.phaseId(), is(1));
        assertThat(streamedNode.nodeIds(), containsInAnyOrder("n1", "n2"));
        assertThat(streamedNode.routing(), equalTo(routing));
        assertThat(streamedNode.distributionInfo(), equalTo(DistributionInfo.DEFAULT_BROADCAST));
    }
}
