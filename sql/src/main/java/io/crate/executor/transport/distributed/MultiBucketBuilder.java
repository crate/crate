/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Throwables;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.StreamBucket;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class MultiBucketBuilder {

    private final List<StreamBucket.Builder> bucketBuilders;

    public MultiBucketBuilder(Streamer<?>[] streamers, int numBuckets) {
        bucketBuilders = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            bucketBuilders.add(new StreamBucket.Builder(streamers));
        }
    }

    public Bucket build(int bucketIdx) {
        Bucket bucket = null;
        try {
            StreamBucket.Builder builder = bucketBuilders.get(bucketIdx);
            synchronized (builder) {
                bucket = builder.build();
                builder.reset();
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }

        return bucket;
    }

    public int size(int bucketIdx) {
        StreamBucket.Builder builder = bucketBuilders.get(bucketIdx);
        synchronized (builder) {
            return builder.size();
        }
    }

    /**
     * get bucket number by doing modulo hashcode of first row-element
     */
    public int getBucket(Row row) {
        int hash;
        if (row.size() > 0) {
            hash = hashCode(row.get(0));
            if (hash == Integer.MIN_VALUE) {
                hash = 0; // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
            }
        } else {
            hash = 0; // if the row is empty/null (is the case if e.g. toCollect is an empty list)
        }
        return Math.abs(hash) % bucketBuilders.size();
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

    public void setNextRow(int index, Row row) throws IOException {
        StreamBucket.Builder builder = bucketBuilders.get(index);
        synchronized (builder) {
            builder.add(row);
        }
    }

}
