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
import io.crate.concurrent.CompletionListenable;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.RamAccounting;

import org.jspecify.annotations.Nullable;
import java.util.concurrent.CompletableFuture;


public class SingleBucketBuilder implements RowConsumer, CompletionListenable<StreamBucket> {

    private final Streamer<?>[] streamers;
    private RamAccounting ramAccounting;
    private final CompletableFuture<StreamBucket> bucketFuture = new CompletableFuture<>();

    public SingleBucketBuilder(Streamer<?>[] streamers, RamAccounting ramAccounting) {
        this.streamers = streamers;
        this.ramAccounting = ramAccounting;
    }

    @Override
    public CompletableFuture<StreamBucket> completionFuture() {
        return bucketFuture;
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (failure == null) {
            StreamBucketCollector streamBucketCollector = new StreamBucketCollector(streamers, ramAccounting);
            BatchIterators.collect(iterator, streamBucketCollector.supplier().get(), streamBucketCollector, bucketFuture);
        } else {
            if (iterator != null) {
                iterator.close();
            }
            bucketFuture.completeExceptionally(failure);
        }
    }
}
