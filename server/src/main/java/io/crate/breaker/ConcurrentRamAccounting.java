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

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;

import io.crate.common.annotations.ThreadSafe;
import io.crate.data.breaker.RamAccounting;

/**
 * A RamAccounting implementation that can be used concurrently.
 * (If the used `reserveBytes`/`releaseBytes` components are thread-safe)
 */
@ThreadSafe
public final class ConcurrentRamAccounting implements RamAccounting {

    private final AtomicLong usedBytes = new AtomicLong(0L);
    private final LongConsumer reserveBytes;
    private final LongConsumer releaseBytes;
    private final String label;
    private final int operationMemoryLimit;

    public static ConcurrentRamAccounting forCircuitBreaker(String label, CircuitBreaker circuitBreaker, int operationMemoryLimit) {
        return new ConcurrentRamAccounting(
            bytes -> circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, label),
            bytes -> circuitBreaker.addWithoutBreaking(- bytes),
            label,
            operationMemoryLimit
        );
    }

    public ConcurrentRamAccounting(LongConsumer reserveBytes,
                                   LongConsumer releaseBytes,
                                   String label,
                                   int operationMemoryLimit) {
        this.reserveBytes = reserveBytes;
        this.releaseBytes = releaseBytes;
        this.label = label;
        this.operationMemoryLimit = operationMemoryLimit;
    }

    @Override
    public void addBytes(long bytes) {
        long currentUsedBytes = usedBytes.addAndGet(bytes);
        if (operationMemoryLimit > 0 && currentUsedBytes > operationMemoryLimit) {
            usedBytes.addAndGet(- bytes);
            throw new CircuitBreakingException(String.format(Locale.ENGLISH,
                "\"%s\" reached operation memory limit. Used: %s, Limit: %s",
                label,
                new ByteSizeValue(currentUsedBytes),
                new ByteSizeValue(operationMemoryLimit)
            ));
        }
        try {
            reserveBytes.accept(bytes);
        } catch (Exception e) {
            usedBytes.addAndGet(- bytes);
            throw e;
        }
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
