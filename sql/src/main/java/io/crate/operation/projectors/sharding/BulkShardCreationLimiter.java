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

package io.crate.operation.projectors.sharding;

import io.crate.analyze.NumberOfReplicas;
import io.crate.executor.transport.ShardRequest;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.function.Predicate;

public class BulkShardCreationLimiter<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements Predicate<ShardedRequests<TReq, TItem>> {

    /**
     * Defines the maximum number of new shards per node which can be safely created before running into the
     * <p>wait_for_active_shards</p> timeout of 30sec. Value was wisely chosen after some brave testing.
     */
    static final int MAX_NEW_SHARDS_PER_NODE = 10;
    private static final Logger LOGGER = Loggers.getLogger(BulkShardCreationLimiter.class);

    private final int numDataNodes;
    private final int numberOfAllShards;

    BulkShardCreationLimiter(Settings tableSettings, int numDataNodes) {
        this.numDataNodes = numDataNodes;
        int numberOfShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(tableSettings);
        int numberOfReplicas = NumberOfReplicas.fromSettings(tableSettings, numDataNodes);
        numberOfAllShards = (numberOfShards + (numberOfShards * numberOfReplicas));
    }

    @Override
    public boolean test(ShardedRequests<TReq, TItem> requests) {
        if (requests.itemsByMissingIndex.isEmpty() == false) {
            int numberOfShardForAllIndices = numberOfAllShards * requests.itemsByMissingIndex.size();
            int numberOfShardsPerNode = numberOfShardForAllIndices / numDataNodes;
            if (numberOfShardsPerNode >= MAX_NEW_SHARDS_PER_NODE) {
                LOGGER.debug("Number of NEW shards per node {} reached maximum limit of {}",
                    numberOfShardsPerNode, MAX_NEW_SHARDS_PER_NODE);
                return true;
            }
        }
        return false;
    }
}
