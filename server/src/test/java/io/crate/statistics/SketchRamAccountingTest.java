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

import static org.apache.lucene.tests.util.LuceneTestCase.expectThrows;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;

import org.apache.lucene.store.RateLimiter;
import org.assertj.core.api.Assertions;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;

public class SketchRamAccountingTest extends ESTestCase {

    private static class TrackingRateLimiter extends RateLimiter.SimpleRateLimiter {

        int pauses = 0;

        public TrackingRateLimiter(long mbPerSecond) {
            super(mbPerSecond);
        }

        @Override
        public long pause(long bytes) {
            pauses++;
            return 0;
        }
    }

    @Test
    public void test_rate_limited_ram_accounting() {
        TrackingRateLimiter rateLimiter = new TrackingRateLimiter(1000);
        var ra = new SketchRamAccounting(RamAccounting.NO_ACCOUNTING, rateLimiter);

        long bytesToAdd = rateLimiter.getMinPauseCheckBytes() / 2;

        for (int i = 0; i < 10; i++) {
            ra.addBytes(bytesToAdd);
        }

        Assertions.assertThat(rateLimiter.pauses).isEqualTo(5);
    }

    @Test
    public void test_rate_limit_disabled() {
        TrackingRateLimiter rateLimiter = new TrackingRateLimiter(1000);
        var ra = new SketchRamAccounting(RamAccounting.NO_ACCOUNTING, rateLimiter);

        long bytesToAdd = rateLimiter.getMinPauseCheckBytes() / 2;

        // disable rate limit
        rateLimiter.setMBPerSec(0);

        for (int i = 0; i < 10; i++) {
            ra.addBytes(bytesToAdd);
        }

        Assertions.assertThat(rateLimiter.pauses).isEqualTo(0);
    }

    @Test
    public void test_throws_exceptions() throws IOException {

        boolean[] closed = new boolean[1];

        RamAccounting a = new RamAccounting() {

            long total;

            @Override
            public void addBytes(long bytes) {
                total += bytes;
                if (total > 32) {
                    throw new RuntimeException("Circuit break! " + total);
                }
            }

            @Override
            public long totalBytes() {
                return total;
            }

            @Override
            public void release() {
                total = 0;
            }

            @Override
            public void close() {
                closed[0] = true;
            }
        };

        TrackingRateLimiter rateLimiter = new TrackingRateLimiter(0);
        try (SketchRamAccounting accounting = new SketchRamAccounting(a, rateLimiter)) {
            accounting.addBytes(1025);  // first 1024 accounted for, next 1 cached
            for (int i = 0; i < 32; i++) {
                accounting.addBytes(1);     // remain in cache
            }
            accounting.addBytes(991);       // still cached

            Exception e = expectThrows(RuntimeException.class, () -> accounting.addBytes(1));
            assertThat(e).hasMessageContaining("Circuit break!");

            a.release();
        }

        assertThat(closed[0]).isTrue();

    }

}
