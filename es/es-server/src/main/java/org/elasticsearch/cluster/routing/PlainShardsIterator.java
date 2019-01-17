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
package org.elasticsearch.cluster.routing;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A simple {@link ShardsIterator} that iterates a list or sub-list of
 * {@link ShardRouting shard indexRoutings}.
 */
public class PlainShardsIterator implements ShardsIterator {

    private final List<ShardRouting> shards;

    // Calls to nextOrNull might be performed on different threads in the transport actions so we need the volatile
    // keyword in order to ensure visibility. Note that it is fine to use `volatile` for a counter in that case given
    // that although nextOrNull might be called from different threads, it can never happen concurrently.
    private volatile int index;

    public PlainShardsIterator(List<ShardRouting> shards) {
        this.shards = shards;
        reset();
    }

    @Override
    public void reset() {
        index = 0;
    }

    @Override
    public int remaining() {
        return shards.size() - index;
    }

    @Override
    public ShardRouting nextOrNull() {
        if (index == shards.size()) {
            return null;
        } else {
            return shards.get(index++);
        }
    }

    @Override
    public int size() {
        return shards.size();
    }

    @Override
    public int sizeActive() {
        int count = 0;
        for (ShardRouting shard : shards) {
            if (shard.active()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public List<ShardRouting> getShardRoutings() {
        return Collections.unmodifiableList(shards);
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return shards.iterator();
    }
}
