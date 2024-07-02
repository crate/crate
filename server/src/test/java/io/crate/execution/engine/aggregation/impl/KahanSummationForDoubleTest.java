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

import io.crate.execution.engine.aggregation.impl.util.KahanSummationForDouble;
import io.crate.operation.aggregation.AggregationTestCase;

public class KahanSummationForDoubleTest extends AggregationTestCase {

    @Test
    public void shouldSumTwoValues() {
        var kahanSummation = new KahanSummationForDouble();
        assertThat(kahanSummation.sum(1.0d, 2.0d)).isEqualTo(3.0d);
    }

    @Test
    public void shouldSumListOfValuesWithBetterPrecision() {
        var kahanSummation = new KahanSummationForDouble();
        double total = 0;
        for (int i = 0; i < 10; i++) {
            total = kahanSummation.sum(total, 0.2);
        }

        // The same operations using '+' returns 1.9999999999999998
        assertThat(total).isEqualTo(2.0d);
    }
}
