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

package io.crate.planner.node;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RowGranularity;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataTypes;

public class MergeNodeTest extends ESTestCase {

    @Test
    public void testSerialization() throws Exception {
        List<Symbol> keys = Collections.singletonList(new InputColumn(0, DataTypes.STRING));
        List<Aggregation> aggregations = Collections.singletonList(
            new Aggregation(
                CountAggregation.COUNT_STAR_SIGNATURE,
                CountAggregation.COUNT_STAR_SIGNATURE.getReturnType().createType(),
                Collections.emptyList()
            )
        );
        GroupProjection groupProjection = new GroupProjection(
            keys, aggregations, AggregateMode.PARTIAL_FINAL, RowGranularity.CLUSTER);
        LimitAndOffsetProjection limitAndOffsetProjection = new LimitAndOffsetProjection(10, 0, Symbols.typeView(groupProjection.outputs()));

        List<Projection> projections = Arrays.asList(groupProjection, limitAndOffsetProjection);
        MergePhase node = new MergePhase(
            UUID.randomUUID(),
            0,
            "merge",
            2,
            1,
            Set.of("node1", "node2"),
            List.of(DataTypes.UNDEFINED, DataTypes.STRING),
            projections,
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );

        BytesStreamOutput output = new BytesStreamOutput();
        node.writeTo(output);


        StreamInput input = output.bytes().streamInput();
        MergePhase node2 = new MergePhase(input);

        assertThat(node.numUpstreams()).isEqualTo(node2.numUpstreams());
        assertThat(node.nodeIds()).isEqualTo(node2.nodeIds());
        assertThat(node.jobId()).isEqualTo(node2.jobId());
        assertThat(node2.inputTypes()).isEqualTo(node.inputTypes());
        assertThat(node.phaseId()).isEqualTo(node2.phaseId());
        assertThat(node.distributionInfo()).isEqualTo(node2.distributionInfo());
    }
}
