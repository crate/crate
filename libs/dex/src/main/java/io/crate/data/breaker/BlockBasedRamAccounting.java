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

package io.crate.data.breaker;

import java.util.function.LongConsumer;

/**
 * A RamAccounting implementation that reserves blocks of memory up-front.
 * This implementation should be used from a single thread only.
 */
public final class BlockBasedRamAccounting implements RamAccounting {

    public static final int MAX_BLOCK_SIZE_IN_BYTES = 2 * 1024 * 1024;

    /**
     * 2 GB × 0.001 ➞ MB
     *   = 2 MB
     *
     * 256 MB × 0.001 ➞ MB
     *
     *   = 0.256 MB
     */
    private static final double BREAKER_LIMIT_PERCENTAGE = 0.001;

    private final LongConsumer reserveMemory;
    private final int blockSizeInBytes;

    private long usedBytes = 0;
    private long reservedBytes = 0;

    /**
     * Scale the block size that is eagerly allocated depending on the circuit breakers current limit and the shards
     * on the node.
     */
    public static int blockSizeInBytesPerShard(long breakerLimit, int shardsUsedOnNode) {
        int blockSize = Math.min((int) (breakerLimit * BREAKER_LIMIT_PERCENTAGE), MAX_BLOCK_SIZE_IN_BYTES);
        return blockSize / Math.max(shardsUsedOnNode, 1);
    }

    public static int blockSizeInBytes(long breakerLimit) {
        return Math.min((int) (breakerLimit * BREAKER_LIMIT_PERCENTAGE), MAX_BLOCK_SIZE_IN_BYTES);
    }

    public BlockBasedRamAccounting(LongConsumer reserveMemory, int blockSizeInBytes) {
        this.reserveMemory = reserveMemory;
        this.blockSizeInBytes = blockSizeInBytes;
    }

    @Override
    public void addBytes(long bytes) {
        usedBytes += bytes;
        if ((reservedBytes - usedBytes) < 0) {
            long reserveBytes = bytes > blockSizeInBytes ? bytes : blockSizeInBytes;
            try {
                reserveMemory.accept(reserveBytes);
            } catch (Exception e) {
                usedBytes -= bytes;
                throw e;
            }
            reservedBytes += reserveBytes;
        }
        assert reservedBytes >= usedBytes : "reservedBytes must be >= usedBytes: " + toString();
    }

    @Override
    public long totalBytes() {
        return usedBytes;
    }

    @Override
    public void release() {
        reserveMemory.accept(- reservedBytes);
        usedBytes = 0;
        reservedBytes = 0;
    }

    @Override
    public void close() {
        release();
    }

    @Override
    public String toString() {
        return "RamAccounting{" +
               "usedBytes=" + usedBytes +
               ", reservedBytes=" + reservedBytes +
               '}';
    }
}
