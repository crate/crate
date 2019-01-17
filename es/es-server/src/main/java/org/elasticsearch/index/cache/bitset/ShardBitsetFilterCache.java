/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.bitset;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

public class ShardBitsetFilterCache extends AbstractIndexShardComponent {

    private final CounterMetric totalMetric = new CounterMetric();

    public ShardBitsetFilterCache(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
    }

    public void onCached(long sizeInBytes) {
        totalMetric.inc(sizeInBytes);
    }

    public void onRemoval(long sizeInBytes) {
        totalMetric.dec(sizeInBytes);
    }

    public long getMemorySizeInBytes() {
        return totalMetric.count();
    }

}
