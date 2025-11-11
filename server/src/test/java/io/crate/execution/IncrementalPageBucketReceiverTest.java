/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.distribution.DistributedResultResponse;
import io.crate.execution.jobs.PageResultListener;

public class IncrementalPageBucketReceiverTest {

    @Test
    public void test_processing_future_completed_when_finisher_throws() {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();
        Collector<Row, ?, Iterable<Row>> collector = Collectors.collectingAndThen(Collectors.toList(), _ -> {
            throw new CircuitBreakingException("dummy"); // Failing finisher
        });
        var pageBucketReceiver = new IncrementalPageBucketReceiver<>(
            collector,
            batchConsumer,
            Runnable::run,
            new Streamer[1],
            1
        );
        pageBucketReceiver.setBucket(0, Bucket.EMPTY, true, _ -> {});
        assertThat(pageBucketReceiver.completionFuture()).completesExceptionallyWithin(1, TimeUnit.SECONDS);
        assertThat(batchConsumer.completionFuture()).completesExceptionallyWithin(1, TimeUnit.SECONDS);
    }

    @Test
    public void test_listener_doesnt_need_more_when_processRows_throws() {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();
        Collector<Row, Object, Iterable<Row>> collector = new Collector<>() {
            @Override
            public Supplier<Object> supplier() {
                return ArrayList::new;
            }

            @Override
            public BiConsumer<Object, Row> accumulator() {
                return (_, _) -> {
                    throw new CircuitBreakingException("dummy");
                };
            }

            @Override
            public BinaryOperator<Object> combiner() {
                return null;
            }

            @Override
            public Function<Object, Iterable<Row>> finisher() {
                return null;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Set.of();
            }
        };

        var pageBucketReceiver = new IncrementalPageBucketReceiver<>(
            collector,
            batchConsumer,
            Runnable::run,
            new Streamer[1],
            1
        );

        // First call goes to currentlyAccumulating == null, needMore must be true after the call
        final CompletableFuture<DistributedResultResponse> result = new CompletableFuture<>();
        PageResultListener listener = needMore -> result.complete(new DistributedResultResponse(needMore));
        pageBucketReceiver.setBucket(0, Bucket.EMPTY, false, listener);
        assertThat(result).isCompletedWithValueMatchingWithin(
            distributedResultResponse -> distributedResultResponse.needMore() == true,
            Duration.ofSeconds(1)
        );

        // Second call goes to currentlyAccumulating != null, use non-empty bucket to provoke CBE
        Bucket bucket = new ArrayBucket(new Object[][]{
            new Object[]{1},
        });
        final CompletableFuture<DistributedResultResponse> result2 = new CompletableFuture<>();
        PageResultListener listener2 = needMore -> result2.complete(new DistributedResultResponse(needMore));
        pageBucketReceiver.setBucket(0, bucket, false, listener2);
        assertThat(result2).isCompletedWithValueMatchingWithin(
            distributedResultResponse -> distributedResultResponse.needMore() == true,
            Duration.ofSeconds(1)
        );

        // Call after failed processRows, listener must see that previous call was completed exceptionally
        final CompletableFuture<DistributedResultResponse> result3 = new CompletableFuture<>();
        PageResultListener listener3 = needMore -> result3.complete(new DistributedResultResponse(needMore));
        pageBucketReceiver.setBucket(0, Bucket.EMPTY, false, listener3);
        assertThat(result3).isCompletedWithValueMatchingWithin(
            distributedResultResponse -> distributedResultResponse.needMore() == false,
            Duration.ofSeconds(1)
        );
    }
}
