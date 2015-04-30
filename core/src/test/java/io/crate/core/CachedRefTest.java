/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.core;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class CachedRefTest extends CrateUnitTest {

    @Test
    public void testZeroCacheAlwaysRefresh() throws Exception {
        CachedRef<Long> longCache = new CachedRef<Long>(0, TimeUnit.MILLISECONDS) {
            private AtomicLong internalLong = new AtomicLong(0L);
            @Override
            protected Long refresh() {
                return internalLong.incrementAndGet();
            }
        };
        long previous = -1L;
        for (int i = 0; i < 1000; i++) {
            long cur = longCache.get();
            assertThat(cur, is(greaterThan(previous)));
            previous = cur;
            Thread.sleep(1);
        }
    }

    @Test
    public void testRefresh() throws Exception {
        CachedRef<Long> longCache = new CachedRef<Long>(10, TimeUnit.MILLISECONDS) {
            private AtomicLong internalLong = new AtomicLong(0L);
            @Override
            protected Long refresh() {
                return internalLong.incrementAndGet();
            }
        };

        long first = longCache.get();
        long second = longCache.get();
        assertThat(first, is(1L));
        assertThat(second, is(first));
        Thread.sleep(20);
        assertThat(longCache.get(), is(2L));
    }
}
