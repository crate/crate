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

package io.crate.execution.jobs;

import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

import io.crate.common.exceptions.Exceptions;
import io.crate.data.breaker.RamAccounting;

/**
 * A {@link DownstreamRXTask} which receives paged buckets from upstreams
 * and forwards the merged bucket results to the consumers for further processing.
 */
public class DistResultRXTask implements Task, DownstreamRXTask {

    private final int id;
    private final String name;
    private final int numBuckets;
    private final PageBucketReceiver pageBucketReceiver;
    private final CompletableFuture<Void> completionFuture;
    private final RamAccounting ramAccounting;

    private long totalBytesUsed = -1;

    public DistResultRXTask(int id,
                            String name,
                            PageBucketReceiver pageBucketReceiver,
                            RamAccounting ramAccounting,
                            int numBuckets) {
        this.id = id;
        this.name = name;
        this.numBuckets = numBuckets;
        this.pageBucketReceiver = pageBucketReceiver;
        this.ramAccounting = ramAccounting;
        this.completionFuture = pageBucketReceiver.completionFuture().handle((result, ex) -> {
            totalBytesUsed = ramAccounting.totalBytes();
            if (ex instanceof IllegalStateException) {
                kill(ex);
            }
            if (ex == null) {
                return null;
            } else {
                throw Exceptions.toRuntimeException(ex);
            }
        });
    }

    @Override
    public void kill(Throwable t) {
        pageBucketReceiver.kill(t);
    }

    @Override
    public CompletableFuture<Void> start() {
        // E.g. If the upstreamPhase is a collectPhase for a partitioned table without any partitions
        // there won't be any executionNodes for that collectPhase
        // -> no upstreams -> just finish
        if (numBuckets == 0) {
            pageBucketReceiver.consumeRows();
        }
        return null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return "DistResultRXTask{" +
               "id=" + id() +
               ", numBuckets=" + numBuckets +
               ", isDone=" + completionFuture.isDone() +
               ", pageBucketReceiver=" + pageBucketReceiver +
               '}';
    }

    /**
     * The default behavior is to receive all upstream buckets,
     * regardless of the input id. For a {@link DownstreamRXTask}
     * which uses the inputId, see {@link JoinTask}.
     */
    @Nullable
    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        return pageBucketReceiver;
    }

    @Override
    public CompletableFuture<Void> completionFuture() {
        return completionFuture;
    }

    @Override
    public long bytesUsed() {
        if (totalBytesUsed == -1) {
            return ramAccounting.totalBytes();
        } else {
            return totalBytesUsed;
        }
    }
}
