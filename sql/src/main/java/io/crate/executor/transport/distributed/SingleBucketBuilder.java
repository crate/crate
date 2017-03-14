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
import io.crate.data.BatchConsumer;
import io.crate.data.BatchRowVisitor;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.executor.transport.StreamBucket;
import io.crate.executor.transport.StreamBucketCollector;
import io.crate.operation.projectors.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public class SingleBucketBuilder implements RowReceiver {

    private final StreamBucket.Builder bucketBuilder;
    private final Streamer<?>[] streamers;
    private final CompletableFuture<Bucket> bucketFuture = new CompletableFuture<>();

    public SingleBucketBuilder(Streamer<?>[] streamers) {
        bucketBuilder = new StreamBucket.Builder(streamers);
        this.streamers = streamers;
    }

    @Override
    public CompletableFuture<Bucket> completionFuture() {
        return bucketFuture;
    }

    @Override
    public Result setNextRow(Row row) {
        try {
            bucketBuilder.add(row);
        } catch (Throwable e) {
            Throwables.propagate(e);
        }
        return Result.CONTINUE;
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        try {
            bucketFuture.complete(bucketBuilder.build());
        } catch (IOException e) {
            bucketFuture.completeExceptionally(e);
        }
    }

    @Override
    public void fail(Throwable throwable) {
        bucketFuture.completeExceptionally(throwable);
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        bucketFuture.completeExceptionally(throwable);
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Nullable
    @Override
    public BatchConsumer asConsumer() {
        return (it, t) -> {
            if (t == null) {
                bucketFuture.whenComplete((ignored, failure) -> it.close());
                StreamBucketCollector streamBucketCollector = new StreamBucketCollector(streamers);
                BatchRowVisitor.visitRows(it, streamBucketCollector.supplier().get(), streamBucketCollector, bucketFuture);
            } else {
                if (it != null) {
                    it.close();
                }
                bucketFuture.completeExceptionally(t);
            }
        };
    }
}
