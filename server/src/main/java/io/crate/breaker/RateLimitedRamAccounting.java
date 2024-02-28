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

package io.crate.breaker;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.lucene.store.RateLimiter;

import io.crate.data.breaker.RamAccounting;

/**
 * Wraps a RamAccounting instance with a RateLimiter, which will pause after
 * a configurable number of bytes have been accounted for.
 */
public class RateLimitedRamAccounting implements RamAccounting {

    long bytesSinceLastPause;

    final RamAccounting in;
    final RateLimiter rateLimiter;

    public static RateLimitedRamAccounting wrap(RamAccounting in, RateLimiter rateLimiter) {
        return new RateLimitedRamAccounting(in, rateLimiter);
    }

    private RateLimitedRamAccounting(RamAccounting in, RateLimiter rateLimiter) {
        this.in = in;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public void addBytes(long bytes) {
        in.addBytes(bytes);
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
    public long totalBytes() {
        return in.totalBytes();
    }

    @Override
    public void release() {
        in.release();
    }

    @Override
    public void close() {
        in.close();
    }
}
