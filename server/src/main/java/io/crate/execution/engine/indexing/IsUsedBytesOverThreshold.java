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

package io.crate.execution.engine.indexing;

import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeUnit;

import io.crate.concurrent.limits.ConcurrencyLimit;

class IsUsedBytesOverThreshold implements Predicate<Accountable> {

    private static final Logger LOGGER = LogManager.getLogger(IsUsedBytesOverThreshold.class);

    private static final long MIN_ACCEPTABLE_BYTES = ByteSizeUnit.KB.toBytes(64);

    private final CircuitBreaker queryCircuitBreaker;
    private final ConcurrencyLimit nodeLimit;

    public IsUsedBytesOverThreshold(CircuitBreaker queryCircuitBreaker, ConcurrencyLimit nodeLimit) {
        this.queryCircuitBreaker = queryCircuitBreaker;
        this.nodeLimit = nodeLimit;
    }

    @Override
    public final boolean test(Accountable accountable) {
        long localJobs = Math.max(1, nodeLimit.numInflight());
        double memoryRatio = 1.0 / localJobs;
        long maxUsableByShardRequests = Math.max(
            (long) (queryCircuitBreaker.getFree() * ShardingUpsertExecutor.BREAKER_LIMIT_PERCENTAGE * memoryRatio), MIN_ACCEPTABLE_BYTES);
        long usedMemoryEstimate = accountable.ramBytesUsed();
        boolean requestsTooBig = usedMemoryEstimate > maxUsableByShardRequests;
        if (requestsTooBig && LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Creating smaller bulk requests because shardedRequests is using too much memory. "
                    + "request={} maxBytesUsableByShardedRequests={}",
                accountable,
                maxUsableByShardRequests
            );
        }
        return requestsTooBig;
    }
}
