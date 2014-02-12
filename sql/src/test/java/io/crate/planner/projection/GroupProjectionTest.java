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

package io.crate.planner.projection;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TestingHelpers;
import io.crate.operator.aggregation.impl.CountAggregation;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Value;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GroupProjectionTest {


    @Test
    public void testStreaming() throws Exception {

        GroupProjection p = new GroupProjection();
        p.keys(
                ImmutableList.<Symbol>of(
                        new Value(DataType.STRING),
                        new Value(DataType.SHORT)
                ));

        p.values(ImmutableList.<Aggregation>of());

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        GroupProjection p2 = (GroupProjection) Projection.fromStream(in);

        assertEquals(p, p2);

    }

    @Test
    public void testStreaming2() throws Exception {
        Reference nameRef = TestingHelpers.createReference("name", DataType.STRING);
        GroupProjection groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(nameRef));
        groupProjection.values(Arrays.asList(
                new Aggregation(
                        new FunctionInfo(new FunctionIdent(CountAggregation.NAME, ImmutableList.<DataType>of()), DataType.LONG),
                        ImmutableList.<Symbol>of(),
                        Aggregation.Step.PARTIAL,
                        Aggregation.Step.FINAL
                )
        ));

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(groupProjection, out);


        BytesStreamInput in = new BytesStreamInput(out.bytes());
        GroupProjection p2 = (GroupProjection) Projection.fromStream(in);

        assertThat(p2.keys.size(), is(1));
        assertThat(p2.values().size(), is(1));
    }
}
