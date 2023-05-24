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

package io.crate.execution.engine.join;

import java.util.function.LongToIntFunction;

import org.elasticsearch.common.breaker.CircuitBreaker;

/**
 * Calculates the number of rows to fit in a block, based on the available memory, the number of rows and the row size.
 */
public class RamBlockSizeCalculator implements LongToIntFunction {

    public static final int FALLBACK_SIZE = 500;

    private final int defaultBlockSize;
    private final CircuitBreaker circuitBreaker;
    private final long estimatedRowSizeForLeft;

    public RamBlockSizeCalculator(int defaultBlockSize,
                                  CircuitBreaker circuitBreaker,
                                  long estimatedRowSizeForLeft) {
        this.defaultBlockSize = defaultBlockSize;
        this.circuitBreaker = circuitBreaker;
        this.estimatedRowSizeForLeft = estimatedRowSizeForLeft;
    }

    @Override
    public int applyAsInt(long averageSizeInBytes) {
        long leftRowSize = averageSizeInBytes > 0 ? averageSizeInBytes : estimatedRowSizeForLeft;
        if (leftRowSize <= 0) {
            return FALLBACK_SIZE;
        }
        if (circuitBreaker.getLimit() == -1) {
            return FALLBACK_SIZE;
        }
        long availableMemory = circuitBreaker.getLimit() - circuitBreaker.getUsed();
        long numRowsFittingIntoAvailableMemory = availableMemory / leftRowSize;

        // Restrict the number of rows per block by whatever is lowest:
        // - numRowsFittingIntoAvailableMemory
        // - defaultBlockSize it is an integer, the blockSize must not exceed Integer.MAX_SIZE to fit into the buffer structure and:
        //      for distributed hash joins, we must ensure that each parallel executed join is switching to the same relation
        //      eventually and does not load the next batch while another is already switching. this would result in a
        //      dead lock caused by the constraint that all receivers must response to the collect nodes before a next batch
        //      is sent.
        int blockSize = (int) Math.min(defaultBlockSize, numRowsFittingIntoAvailableMemory);

        // In case no mem available from circuit breaker then still allocate a small blockSize,
        // so that at least some rows (min 10) could be processed and a CircuitBreakerException can be triggered.
        return blockSize <= 0 ? 10 : blockSize;
    }
}
