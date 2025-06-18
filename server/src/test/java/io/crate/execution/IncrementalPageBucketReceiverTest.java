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

import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;

public class IncrementalPageBucketReceiverTest {

    @Test
    public void test_processing_future_completed_when_finisher_throws() {
        TestingRowConsumer batchConsumer = new TestingRowConsumer();
        Collector<Row, Object, Iterable<Row>> collector = Collectors.collectingAndThen(Collectors.toList(), _ -> {
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
}
