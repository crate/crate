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

package io.crate.execution.engine.aggregation.impl.average.numeric;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.MathContext;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.execution.engine.aggregation.impl.util.BigDecimalValueWrapper;

public class NumericAverageStateTest {

    @Test
    @SuppressWarnings("unchecked")
    public void test_can_stream_bigdecimal_with_precision_eq_scale() throws Exception {
        var type = new NumericAverageStateType();
        var out = new BytesStreamOutput();
        var bigDecimal = new BigDecimal("0.25", new MathContext(2));
        var bigDecimalValueWrapper = new BigDecimalValueWrapper(bigDecimal);
        var numericAverageState = new NumericAverageState<>(bigDecimalValueWrapper, 1);
        type.writeValueTo(out, numericAverageState);

        StreamInput in = out.bytes().streamInput();
        NumericAverageState<BigDecimalValueWrapper> valueFrom = type.readValueFrom(in);
        assertThat(valueFrom.sum.value()).isEqualTo(bigDecimal);

    }
}

