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

package io.crate.execution.engine.aggregation.impl;


import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.crate.execution.engine.aggregation.impl.util.KahanSummationForFloat;
import io.crate.operation.aggregation.AggregationTestCase;

public class KahanSummationForFloatTest extends AggregationTestCase {

    @Test
    public void shouldSumTwoValues() {
        var kahanSummation = new KahanSummationForFloat();
        assertThat(kahanSummation.sum(1.0f, 2.0f)).isEqualTo(3.0f);
    }

    @Test
    public void shouldSumListOfValuesWithBetterPrecision() {
        var kahanSummation = new KahanSummationForFloat();
        float total = 0;
        for (int i = 0; i < 10; i++) {
            total = kahanSummation.sum(total, 0.2f);
        }

        // The same operations using '+' returns 2.0000002
        assertThat(total).isEqualTo(2.0f);
    }
}
