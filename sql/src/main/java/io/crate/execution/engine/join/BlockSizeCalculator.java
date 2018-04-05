/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.join;

import io.crate.data.Paging;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.function.Supplier;

class BlockSizeCalculator implements Supplier<Integer> {

    static final int DEFAULT_BLOCK_SIZE = Paging.PAGE_SIZE;

    private final CircuitBreaker circuitBreaker;
    private final long estimatedRowSizeForLeft;
    private final long numberOfRowsForLeft;
    private final int rowsToBeConsumed;

    BlockSizeCalculator(CircuitBreaker circuitBreaker,
                        long estimatedRowSizeForLeft,
                        long numberOfRowsForLeft,
                        int rowsToBeConsumed) {
        this.circuitBreaker = circuitBreaker;
        this.estimatedRowSizeForLeft = estimatedRowSizeForLeft;
        this.numberOfRowsForLeft = numberOfRowsForLeft;
        this.rowsToBeConsumed = rowsToBeConsumed;
    }

    int calculateBlockSize() {
        if (statisticsUnavailable(circuitBreaker, estimatedRowSizeForLeft, numberOfRowsForLeft)) {
            return DEFAULT_BLOCK_SIZE;
        }

        int blockSize = (int) Math.min(Integer.MAX_VALUE, (circuitBreaker.getLimit() - circuitBreaker.getUsed()) / estimatedRowSizeForLeft);
        blockSize = (int) Math.min(numberOfRowsForLeft, blockSize);
        // we can encounter operations with explicit limit statement but without any order by clause.
        // in this case, if the limit is much lower than the default block size, we'll try to keep the block size
        // smaller than what the given node can handle as we will likely need to consume a smaller number of rows
        // (imagine a node which can load the entire table in memory so the block size could be in the range of
        // millions - the table row count - but the operation has `limit 100`)
        if (rowsToBeConsumed < Integer.MAX_VALUE && rowsToBeConsumed <= DEFAULT_BLOCK_SIZE) {
            blockSize = Math.min(DEFAULT_BLOCK_SIZE, blockSize);
        }

        // In case no mem available from circuit breaker then still allocate a small blockSize,
        // so that at least some rows (min 1) could be processed and a CircuitBreakerException can be triggered.
        return blockSize <= 0 ? 10 : blockSize;
    }

    private static boolean statisticsUnavailable(CircuitBreaker circuitBreaker,
                                                 long estimatedRowSizeForLeft,
                                                 long numberOfRowsForLeft) {
        return estimatedRowSizeForLeft <= 0 || numberOfRowsForLeft <= 0 || circuitBreaker.getLimit() == -1;
    }

    @Override
    public Integer get() {
        return calculateBlockSize();
    }
}
