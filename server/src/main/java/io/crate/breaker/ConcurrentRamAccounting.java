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

package io.crate.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * A RamAccounting implementation that can be used concurrently.
 * (If the used `reserveBytes`/`releaseBytes` components are thread-safe)
 */
@ThreadSafe
public final class ConcurrentRamAccounting implements RamAccounting {

    private final AtomicLong usedBytes = new AtomicLong(0L);
    private final LongConsumer reserveBytes;
    private final LongConsumer releaseBytes;

    public static ConcurrentRamAccounting forCircuitBreaker(String label, CircuitBreaker circuitBreaker) {
        return new ConcurrentRamAccounting(
            bytes -> circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, label),
            bytes -> circuitBreaker.addWithoutBreaking(- bytes)
        );
    }

    public ConcurrentRamAccounting(LongConsumer reserveBytes, LongConsumer releaseBytes) {
        this.reserveBytes = reserveBytes;
        this.releaseBytes = releaseBytes;
    }

    @Override
    public void addBytes(long bytes) {
        reserveBytes.accept(bytes);
        usedBytes.addAndGet(bytes);
    }

    @Override
    public long totalBytes() {
        return usedBytes.get();
    }

    @Override
    public void release() {
        long prevUsedBytes = usedBytes.getAndSet(0);
        releaseBytes.accept(prevUsedBytes);
    }

    @Override
    public void close() {
        release();
    }
}
