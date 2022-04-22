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

package io.crate.execution.engine.collect.collectors;

import io.crate.data.Killable;
import io.crate.data.Row;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.function.Supplier;

public abstract class OrderedDocCollector implements Supplier<KeyIterable<ShardId, Row>>, AutoCloseable, Killable {

    private final ShardId shardId;
    protected final KeyIterable<ShardId, Row> empty;

    boolean exhausted = false;

    public static OrderedDocCollector empty(ShardId shardId) {
        return new OrderedDocCollector(shardId) {

            @Override
            protected KeyIterable<ShardId, Row> collect() {
                return empty;
            }

            @Override
            public boolean exhausted() {
                return true;
            }
        };
    }

    OrderedDocCollector(ShardId shardId) {
        this.shardId = shardId;
        empty = new KeyIterable<>(shardId, Collections.<Row>emptyList());
    }

    public ShardId shardId() {
        return shardId;
    }

    @Override
    public void close() {
    }

    @Override
    public void kill(@Nonnull Throwable t) {
    }

    /**
     * Returns an iterable for a batch of rows. In order to consume all rows of this collector,
     * {@code #get()} needs to be called while {@linkplain #exhausted()} is {@code false}.
     * After {@linkplain #exhausted()} is {@code true}, all subsequent calls to {@code #get()}
     * will return an empty iterable.
     *
     * @return an iterable for the next batch of rows.
     */
    @Override
    public KeyIterable<ShardId, Row> get() {
        return collect();
    }

    protected abstract KeyIterable<ShardId, Row> collect();

    /**
     * Returns {@code true} if this collector has no rows to deliver anymore.
     */
    public boolean exhausted() {
        return exhausted;
    }

    public KeyIterable<ShardId, Row> empty() {
        return empty;
    }
}
