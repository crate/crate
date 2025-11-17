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

import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.collect.CollectExpression;

/**
 * builds N buckets where N is passed in the CTOR.
 * The rows that are added via {@link #add(Row)} are assigned to the buckets by modulo calculation.
 */
public class ModuloBucketBuilder implements MultiBucketBuilder {

    private final int numBuckets;
    private final List<StreamBucket.Builder> bucketBuilders;
    private final Input<?> distributeBy;
    private final List<CollectExpression<Row, ?>> expressions;
    private int size = 0;

    public ModuloBucketBuilder(Streamer<?>[] streamers,
                               int numBuckets,
                               Input<?> distributeBy,
                               List<CollectExpression<Row, ?>> expressions,
                               RamAccounting ramAccounting) {
        this.numBuckets = numBuckets;
        this.distributeBy = distributeBy;
        this.expressions = expressions;
        this.bucketBuilders = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            bucketBuilders.add(new StreamBucket.Builder(streamers, ramAccounting));
        }
    }

    @Override
    public void add(Row row) {
        int bucket = getBucket(row);
        StreamBucket.Builder builder = bucketBuilders.get(bucket);
        builder.add(row);
        size++;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void build(StreamBucket[] buckets) {
        assert buckets.length == numBuckets : "length of the provided array must match numBuckets";
        for (int i = 0; i < numBuckets; i++) {
            StreamBucket.Builder builder = bucketBuilders.get(i);
            buckets[i] = builder.build();
            builder.reset();
        }
        size = 0;
    }

    /**
     * get bucket number by doing modulo hashcode of the defined row-element
     */
    private int getBucket(Row row) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
        Object value = distributeBy.value();
        int hash = hashCode(value);
        if (hash == Integer.MIN_VALUE) {
            hash = 0; // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
        }
        int bucketIdx = Math.abs(hash) % numBuckets;
        return bucketIdx;
    }

    private static int hashCode(@Nullable Object value) {
        if (value == null) {
            return 0;
        }
        return value.hashCode();
    }

    @Override
    public long ramBytesUsed() {
        long sum = 0;
        for (int i = 0; i < bucketBuilders.size(); i++) {
            sum += bucketBuilders.get(i).ramBytesUsed();
        }
        return sum;
    }
}
