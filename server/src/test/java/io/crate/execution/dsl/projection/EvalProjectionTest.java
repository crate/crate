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

package io.crate.execution.dsl.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.metadata.RowGranularity;

public class EvalProjectionTest extends ESTestCase {

    @Test
    public void test_granularity_property_is_serialized() throws Exception {
        var projection = new EvalProjection(List.of(Literal.of(10)), RowGranularity.SHARD);
        var out = new BytesStreamOutput();
        projection.writeTo(out);

        var in = out.bytes().streamInput();
        var inProjection = new EvalProjection(in);
        assertThat(projection.requiredGranularity()).isEqualTo(inProjection.requiredGranularity());
        assertThat(projection.outputs()).isEqualTo(inProjection.outputs());
    }
}
