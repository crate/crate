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

package io.crate.execution.engine.distribution;

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.data.Row;

/**
 * MultiBucketBuilder that returns N buckets where N is the number of buckets specified in the constructor.
 * Internally only one bucket is built - the same instance is returned N number of times.
 */
public class BroadcastingBucketBuilder implements MultiBucketBuilder {

    private final int numBuckets;
    private final StreamBucket.Builder bucketBuilder;

    public BroadcastingBucketBuilder(Streamer<?>[] streamers, int numBuckets, RamAccounting ramAccounting) {
        this.numBuckets = numBuckets;
        this.bucketBuilder = new StreamBucket.Builder(streamers, ramAccounting);
    }

    @Override
    public void add(Row row) {
        bucketBuilder.add(row);
    }

    @Override
    public int size() {
        return bucketBuilder.size();
    }

    @Override
    public long ramBytesUsed() {
        return bucketBuilder.ramBytesUsed();
    }

    @Override
    public void build(StreamBucket[] buckets) {
        assert buckets.length == numBuckets : "length of the provided array must match numBuckets";
        StreamBucket bucket = bucketBuilder.build();
        bucketBuilder.reset();
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = bucket;
        }
    }
}
