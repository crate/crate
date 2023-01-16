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

package io.crate.statistics;


import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.Random;

import org.junit.Test;

public class ReservoirTest {

    @Test
    public void test_sampling() {
        Random random = new Random(42);
        Reservoir samples = new Reservoir(5, random);
        for (int i = 0; i < 100; i++) {
            samples.update(i);
        }
        assertThat(samples.samples().buffer).contains(83, 50, 13, 18, 38);
    }

    @Test
    public void test_reservoir_is_protected_against_integer_overflow() throws Exception {
        Random random = new Random(42);

        Reservoir samples = new Reservoir(5, random);
        Field f1 = samples.getClass().getDeclaredField("itemsSeen");
        f1.setAccessible(true);
        int itemsSeen = Integer.MAX_VALUE;
        f1.set(samples, itemsSeen);
        samples.update(6L);

        assertThat(samples.samples()).isEmpty();
    }
}
