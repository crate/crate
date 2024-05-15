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

package io.crate.planner.node.dql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.common.collections.MapBuilder;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.Routing;
import io.crate.planner.distribution.DistributionInfo;

public class CountPhaseTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        Routing routing = new Routing(
            MapBuilder.<String, Map<String, IntIndexedContainer>>treeMapBuilder()
                .put("n1", MapBuilder.<String, IntIndexedContainer>treeMapBuilder()
                    .put("i1", IntArrayList.from(1, 2))
                    .put("i2", IntArrayList.from(1, 2)).map())
                .put("n2", MapBuilder.<String, IntIndexedContainer>treeMapBuilder()
                    .put("i1", IntArrayList.from(3)).map()).map());
        CountPhase countPhase = new CountPhase(1, routing, Literal.BOOLEAN_TRUE, DistributionInfo.DEFAULT_BROADCAST);

        BytesStreamOutput out = new BytesStreamOutput(10);
        countPhase.writeTo(out);

        StreamInput in = out.bytes().streamInput();

        CountPhase streamedNode = new CountPhase(in);

        assertThat(streamedNode.phaseId()).isEqualTo(1);
        assertThat(streamedNode.nodeIds(), containsInAnyOrder("n1", "n2"));
        assertThat(streamedNode.routing()).isEqualTo(routing);
        assertThat(streamedNode.distributionInfo()).isEqualTo(DistributionInfo.DEFAULT_BROADCAST);
    }
}
