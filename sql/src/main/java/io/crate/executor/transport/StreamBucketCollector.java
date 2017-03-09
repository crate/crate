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

package io.crate.executor.transport;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Collector implementation that collects Rows creating a (Stream)Bucket
 */
public class StreamBucketCollector implements Collector<Row, StreamBucket.Builder, Bucket> {

    private final Streamer<?>[] streamers;

    public StreamBucketCollector(Streamer<?>[] streamers) {
        this.streamers = streamers;
    }

    @Override
    public Supplier<StreamBucket.Builder> supplier() {
        return () -> new StreamBucket.Builder(streamers);
    }

    @Override
    public BiConsumer<StreamBucket.Builder, Row> accumulator() {
        return (builder, row) -> {
            try {
                builder.add(row);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public BinaryOperator<StreamBucket.Builder> combiner() {
        return (builder1, builder2) -> { throw new UnsupportedOperationException("combine not supported"); };
    }

    @Override
    public Function<StreamBucket.Builder, Bucket> finisher() {
        return (builder) -> {
            try {
                return builder.build();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }
}
