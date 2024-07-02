/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.statistics;

import static io.crate.testing.Asserts.assertThat;

import java.util.stream.IntStream;

import org.elasticsearch.test.ESTestCase;

import io.crate.types.IntegerType;

public class MostCommonValuesTest extends ESTestCase {

    public void test_cutoffs() {
        MostCommonValuesSketch<Integer> sketch = new MostCommonValuesSketch<>(IntegerType.INSTANCE);
        IntStream.range(1, 200).forEach(sketch::update);
        for (int i = 0; i < 10; i++) {
            sketch.update(10);
            sketch.update(20);
        }
        sketch.update(10);

        var mcv = sketch.toMostCommonValues(221, 200);
        assertThat(mcv.values()).containsExactly(10, 20);
    }

}
