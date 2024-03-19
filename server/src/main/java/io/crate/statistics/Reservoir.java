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

import java.util.Arrays;
import java.util.Random;

import com.carrotsearch.hppc.LongArrayList;

/**
 * Reservoir sampling as described in http://rosettacode.org/wiki/Knuth%27s_algorithm_S
 */
public final class Reservoir {

    private final LongArrayList samples;
    private final int maxSamples;
    private final Random random;

    private int itemsSeen = 0;

    public Reservoir(int maxSamples, Random random) {
        this.samples = new LongArrayList(maxSamples);
        this.maxSamples = maxSamples;
        this.random = random;
    }

    public boolean update(long item) {
        if (itemsSeen == Integer.MAX_VALUE) {
            return false;
        }
        itemsSeen++;
        if (itemsSeen <= maxSamples) {
            samples.add(item);
        } else if (random.nextInt(itemsSeen) < maxSamples) {
            samples.set(random.nextInt(maxSamples), item);
        }
        return true;
    }

    public LongArrayList samples() {
        Arrays.sort(samples.buffer, 0, samples.elementsCount);
        return samples;
    }
}
