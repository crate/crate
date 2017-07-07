/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.distributed;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.executor.transport.StreamBucket;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * builds N buckets where N is passed in the CTOR.
 * The rows that are added via {@link #add(Row)} are assigned to the buckets by modulo calculation.
 */
public class ModuloBucketBuilder implements MultiBucketBuilder {

    private final int numBuckets;
    private final List<StreamBucket.Builder> bucketBuilders;
    private final int distributedByColumnIdx;
    private volatile int size = 0;

    public ModuloBucketBuilder(Streamer<?>[] streamers, int numBuckets, int distributedByColumnIdx) {
        this.numBuckets = numBuckets;
        this.distributedByColumnIdx = distributedByColumnIdx;
        this.bucketBuilders = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            bucketBuilders.add(new StreamBucket.Builder(streamers, null));
        }
    }

    @Override
    public void add(Row row) {
        final StreamBucket.Builder builder = bucketBuilders.get(getBucket(row));
        try {
            synchronized (this) {
                builder.add(row);
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
        for (int i = 0; i < numBuckets; i++) {
            try {
                final StreamBucket.Builder builder = bucketBuilders.get(i);
                buckets[i] = builder.build();
                builder.reset();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        size = 0;
    }

    /**
     * get bucket number by doing modulo hashcode of the defined row-element
     */
    private int getBucket(Row row) {
        int hash = hashCode(row.get(distributedByColumnIdx));
        if (hash == Integer.MIN_VALUE) {
            hash = 0; // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
        }
        return Math.abs(hash) % numBuckets;
    }

    private static int hashCode(@Nullable Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof BytesRef) {
            // since lucene 4.8
            // BytesRef.hashCode() uses a random seed across different jvm
            // which causes the hashCode / routing to be different on each node
            // this breaks the group by redistribution logic - need to use a fixed seed here
            // to be consistent.
            return StringHelper.murmurhash3_x86_32(((BytesRef) value), 1);
        }
        return value.hashCode();
    }
}
