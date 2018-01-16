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

package io.crate.execution.engine.distribution;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row;

import java.io.IOException;

/**
 * MultiBucketBuilder that returns N buckets where N is the number of buckets specified in the constructor.
 * Internally only one bucket is built - the same instance is returned N number of times.
 */
public class BroadcastingBucketBuilder implements MultiBucketBuilder {

    private final int numBuckets;
    private final StreamBucket.Builder bucketBuilder;
    private volatile int size = 0;

    public BroadcastingBucketBuilder(Streamer<?>[] streamers, int numBuckets) {
        this.numBuckets = numBuckets;
        this.bucketBuilder = new StreamBucket.Builder(streamers, null);
    }

    @Override
    public void add(Row row) {
        try {
            synchronized (this) {
                bucketBuilder.add(row);
                size++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public synchronized void build(Bucket[] buckets) {
        assert buckets.length == numBuckets : "length of the provided array must match numBuckets";
        final Bucket bucket;
        try {
            bucket = bucketBuilder.build();
            bucketBuilder.reset();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = bucket;
        }
        size = 0;
    }
}
