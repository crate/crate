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

package io.crate.es.indices.fielddata.cache;

import org.apache.lucene.util.Accountable;
import io.crate.es.common.breaker.CircuitBreaker;
import io.crate.es.common.inject.Inject;
import io.crate.es.index.fielddata.IndexFieldDataCache;
import io.crate.es.index.shard.ShardId;
import io.crate.es.indices.breaker.CircuitBreakerService;

/**
 * A {@link io.crate.es.index.fielddata.IndexFieldDataCache.Listener} implementation that updates indices (node) level statistics / service about
 * field data entries being loaded and unloaded.
 *
 * Currently it only decrements the memory used in the  {@link CircuitBreakerService}.
 */
public class IndicesFieldDataCacheListener implements IndexFieldDataCache.Listener {

    private final CircuitBreakerService circuitBreakerService;

    @Inject
    public IndicesFieldDataCacheListener(CircuitBreakerService circuitBreakerService) {
        this.circuitBreakerService = circuitBreakerService;
    }

    @Override
    public void onCache(ShardId shardId, String fieldName, Accountable fieldData) {
    }

    @Override
    public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
        assert sizeInBytes >= 0 : "When reducing circuit breaker, it should be adjusted with a number higher or equal to 0 and not [" + sizeInBytes + "]";
        circuitBreakerService.getBreaker(CircuitBreaker.FIELDDATA).addWithoutBreaking(-sizeInBytes);
    }

}

