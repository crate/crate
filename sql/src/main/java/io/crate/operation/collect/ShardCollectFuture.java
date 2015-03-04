/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect;

import com.google.common.util.concurrent.AbstractFuture;
import io.crate.core.collections.Bucket;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * future that is set after configured number of shards signal
 * that they have finished collecting.
 */
public abstract class ShardCollectFuture extends AbstractFuture<Bucket> {

    private final AtomicInteger numShards;
    protected final AtomicReference<Throwable> lastException = new AtomicReference<>();

    public ShardCollectFuture(int numShards) {
        this.numShards = new AtomicInteger(numShards);
    }

    public void shardFinished() {
        if (numShards.decrementAndGet() <= 0) {
            onAllShardsFinished();
        }
    }

    public void shardFailure(Throwable t) {
        lastException.set(t);
        setException(t);
        if (numShards.decrementAndGet() <= 0) {
            onAllShardsFinished();
        }
    }

    public int numShards() {
        return numShards.get();
    }

    /**
     * take action when all shards finished collecting
     * and all data is completely put into projectors
     */
    public abstract void onAllShardsFinished();
}
