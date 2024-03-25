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

    /**
     * This number is from PostgreSQL, they chose this based on the paper
     * "Random sampling for histogram construction: how much is enough?"
     *
     * > Their Corollary 1 to Theorem 5 says that for table size n, histogram size k,
     * > maximum relative error in bin size f, and error probability gamma, the minimum random sample size is
     * >    r = 4 * k * ln(2*n/gamma) / f^2
     * > Taking f = 0.5, gamma = 0.01, n = 10^6 rows, we obtain r = 305.82 * k
     * > Note that because of the log function, the dependence on n is quite weak;
     * > even at n = 10^12, a 300*k sample gives <= 0.66 bin size error with probability 0.99.
     * > So there's no real need to scale for n, which is a good thing because we don't necessarily know it at this point.
     *
     * In PostgreSQL `k` is configurable (per column). We don't support changing k, we default it to 100
     */
    public static final int NUM_SAMPLES = 30_000;

    private final LongArrayList samples;
    private final int maxSamples;
    private final Random random;

    private int itemsSeen = 0;

    public Reservoir(Random random) {
        this(NUM_SAMPLES, random);
    }

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
