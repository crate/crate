/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dsl.projection;

import com.google.common.collect.ImmutableList;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.Is.is;

public class GroupProjectionTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        ImmutableList<Symbol> keys = ImmutableList.of(
            new InputColumn(0, DataTypes.STRING),
            new InputColumn(1, DataTypes.SHORT)
        );
        ImmutableList<Aggregation> aggregations = ImmutableList.of();
        GroupProjection p = new GroupProjection(keys, aggregations, AggregateMode.ITER_FINAL, RowGranularity.CLUSTER);

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        StreamInput in = out.bytes().streamInput();
        GroupProjection p2 = (GroupProjection) Projection.fromStream(in);

        assertEquals(p, p2);
    }

    @Test
    public void testStreaming2() throws Exception {
        List<Symbol> keys = Collections.singletonList(new InputColumn(0, DataTypes.STRING));
        List<Aggregation> aggregations = Collections.singletonList(
            new Aggregation(
                CountAggregation.COUNT_STAR_FUNCTION,
                CountAggregation.COUNT_STAR_SIGNATURE,
                CountAggregation.COUNT_STAR_FUNCTION.returnType(),
                Collections.emptyList()
            )
        );
        GroupProjection groupProjection = new GroupProjection(
            keys, aggregations, AggregateMode.ITER_FINAL, RowGranularity.CLUSTER);

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(groupProjection, out);


        StreamInput in = out.bytes().streamInput();
        GroupProjection p2 = (GroupProjection) Projection.fromStream(in);

        assertThat(p2.keys().size(), is(1));
        assertThat(p2.values().size(), is(1));
    }

    @Test
    public void testStreamingGranularity() throws Exception {
        ImmutableList<Symbol> keys = ImmutableList.of(
            new InputColumn(0, DataTypes.STRING),
            new InputColumn(1, DataTypes.SHORT)
        );
        ImmutableList<Aggregation> aggregations = ImmutableList.of();
        GroupProjection p = new GroupProjection(keys, aggregations, AggregateMode.ITER_FINAL, RowGranularity.SHARD);
        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        StreamInput in = out.bytes().streamInput();
        GroupProjection p2 = (GroupProjection) Projection.fromStream(in);
        assertEquals(p, p2);
    }
}
