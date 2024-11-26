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

public class BulkShardCreationLimiter implements Predicate<ShardedRequests<?, ?>> {

    /**
     * Defines the maximum number of new shards per node which can be safely created before running into the
     * <p>wait_for_active_shards</p> timeout of 30sec. Value was wisely chosen after some brave testing.
     */
    static final int MAX_NEW_SHARDS_PER_NODE = 10;
    private static final Logger LOGGER = LogManager.getLogger(BulkShardCreationLimiter.class);

    private final int numDataNodes;
    private final int numberOfAllShards;

    BulkShardCreationLimiter(int numberOfShards, int numberOfReplicas, int numDataNodes) {
        this.numDataNodes = numDataNodes;
        this.numberOfAllShards = (numberOfShards + (numberOfShards * numberOfReplicas));
    }

    @Override
    public boolean test(ShardedRequests<?, ?> requests) {
        if (requests.itemsByMissingPartition.isEmpty() == false) {
            int numberOfShardForAllIndices = numberOfAllShards * requests.itemsByMissingPartition.size();
            int numberOfShardsPerNode = numberOfShardForAllIndices / numDataNodes;
            if (numberOfShardsPerNode >= MAX_NEW_SHARDS_PER_NODE) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Number of NEW shards per node {} reached maximum limit of {}",
                        numberOfShardsPerNode, MAX_NEW_SHARDS_PER_NODE);
                }
                return true;
            }
        }
        return false;
    }
}
