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
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Collector implementation that collects Rows creating a StreamBucket
 */
public class StreamBucketCollector implements Collector<Row, StreamBucket.Builder, StreamBucket> {

    private final Streamer<?>[] streamers;
    private final RamAccounting ramAccounting;

    public StreamBucketCollector(Streamer<?>[] streamers, RamAccounting ramAccounting) {
        this.streamers = streamers;
        this.ramAccounting = ramAccounting;
    }

    @Override
    public Supplier<StreamBucket.Builder> supplier() {
        return () -> new StreamBucket.Builder(streamers, ramAccounting);
    }

    @Override
    public BiConsumer<StreamBucket.Builder, Row> accumulator() {
        return StreamBucket.Builder::add;
    }

    @Override
    public BinaryOperator<StreamBucket.Builder> combiner() {
        return (builder1, builder2) -> {
            throw new UnsupportedOperationException("combine not supported");
        };
    }

    @Override
    public Function<StreamBucket.Builder, StreamBucket> finisher() {
        return StreamBucket.Builder::build;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }
}
