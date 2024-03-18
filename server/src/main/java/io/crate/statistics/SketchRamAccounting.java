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

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.lucene.store.RateLimiter;

import io.crate.data.breaker.RamAccounting;

/**
 * Keep track of sketch memory usage, with a rate limit on how much data is being read
 * <p/>
 * Experiments show that using data sketches holding approximately 100 items
 * use between 0.02% and 0.04% of the memory used by an equivalent java array
 * of 30,000 items.
 */
public class SketchRamAccounting implements AutoCloseable {

    private static final int BLOCK_SIZE = 1024;
    private static final int SHIFTED_BLOCK_SIZE = 32;

    private final RamAccounting ramAccounting;
    private final RateLimiter rateLimiter;

    private long blockCache;
    private long bytesSinceLastPause;

    public SketchRamAccounting(RamAccounting ramAccounting, RateLimiter rateLimiter) {
        this.ramAccounting = ramAccounting;
        this.rateLimiter = rateLimiter;
    }

    public void addBytes(long bytes) {
        this.blockCache += bytes;
        while (this.blockCache > BLOCK_SIZE) {
            this.blockCache -= BLOCK_SIZE;
            ramAccounting.addBytes(SHIFTED_BLOCK_SIZE);
        }
        checkRateLimit(bytes);
    }

    private void checkRateLimit(long bytes) {
        if (rateLimiter.getMBPerSec() > 0) {
            // Throttling is enabled
            bytesSinceLastPause += bytes;
            if (bytesSinceLastPause >= rateLimiter.getMinPauseCheckBytes()) {
                try {
                    rateLimiter.pause(bytesSinceLastPause); // SimpleRateLimiter does one volatile read of mbPerSec.
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    bytesSinceLastPause = 0;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        ramAccounting.close();
    }
}
