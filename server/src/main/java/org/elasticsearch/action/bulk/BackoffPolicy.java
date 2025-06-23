/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Gatherer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import io.crate.common.MutableLong;
import io.crate.common.concurrent.ConcurrencyLimit;
import io.crate.common.unit.TimeValue;

/// Provides backoff policies for anything doing retries
/// The policies are exposed as `Iterables` in order to be consumed multiple times.
///
/// - Use [java.util.Iterator#hasNext()] to see if another retry is possible
/// - Use [java.util.Iterator#next()] to get the next delay
public final class BackoffPolicy {

    private BackoffPolicy() {
    }

    private static final int DEFAULT_RETRY_LIMIT = 10;

    public static Iterable<TimeValue> unlimitedDynamic(ConcurrencyLimit concurrencyLimit) {
        return () -> Stream.generate(() -> TimeValue.timeValueNanos(concurrencyLimit.getLastRtt(TimeUnit.NANOSECONDS)))
            .iterator();
    }

    public static Iterable<TimeValue> limitedDynamic(ConcurrencyLimit concurrencyLimit) {
        return () -> Stream.generate(() -> TimeValue.timeValueNanos(concurrencyLimit.getLastRtt(TimeUnit.NANOSECONDS)))
            .limit(DEFAULT_RETRY_LIMIT)
            .iterator();
    }

    private static long exp(long initialDelayMs, long i) {
        long delta = 10 * ((long) Math.exp(0.8d * i) - 1);
        long result = initialDelayMs + delta;

        // with i=52 we get negative: -6788035485022974986;
        // at some point we'd go positive again, using a lower than expected delay.
        return result < 0 || i > 51 ? Long.MAX_VALUE : result;
    }

    private static Stream<TimeValue> exponential(TimeValue initialDelay) {
        long initialDelayMs = initialDelay.millis();
        return LongStream.iterate(1, i -> i + 1)
            .mapToObj(i -> TimeValue.timeValueMillis(exp(initialDelayMs, i)));
    }


    /**
     * Creates an new exponential backoff policy with a default configuration of 50 ms initial wait period and 8 retries taking
     * roughly 5.1 seconds in total.
     *
     * @return A backoff policy with an exponential increase in wait time for retries. The returned instance is thread safe but each
     * iterator created from it should only be used by a single thread.
     */
    public static Iterable<TimeValue> exponentialBackoff() {
        return exponentialBackoff(TimeValue.timeValueMillis(50), 8);
    }

    /**
     * Creates an new exponential backoff policy with the provided configuration.
     *
     * @param initialDelay       The initial delay defines how long to wait for the first retry attempt. Must not be null.
     *                           Must be &lt;= <code>Integer.MAX_VALUE</code> ms.
     * @param maxNumberOfRetries The maximum number of retries. Must be a non-negative number.
     * @return A backoff policy with an exponential increase in wait time for retries. The returned instance is thread safe but each
     * iterator created from it should only be used by a single thread.
     */
    public static Iterable<TimeValue> exponentialBackoff(TimeValue initialDelay, int maxNumberOfRetries) {
        return () -> exponential(initialDelay)
            .limit(maxNumberOfRetries)
            .iterator();
    }

    /// Similar to [#exponentialBackoff(TimeValue, int)] but with a max amount
    /// of total delays passed limit (`timeout`) instead of a fixed number of retries
    public static Iterable<TimeValue> exponentialBackoff(TimeValue initialDelay, TimeValue timeout) {
        long timeoutMs = timeout.millis();
        if (timeoutMs == 0) {
            return List.of();
        }
        Gatherer<TimeValue, MutableLong, TimeValue> withTimeLimit = Gatherer.ofSequential(
            () -> new MutableLong(0),
            (sumState, delay, downstream) -> {
                sumState.add(delay.millis());
                downstream.push(delay);
                long delaySum = sumState.value();
                // if .add overflows and goes negative we also stop
                return delaySum > 0 && delaySum < timeoutMs;
            }
        );
        return () -> exponential(initialDelay)
            .gather(withTimeLimit)
            .iterator();
    }

    /**
     * Creates an new exponential backoff policy with the provided configuration.
     *
     * @param initialDelayMs     The initial delay defines how long to wait for the first retry attempt. Must not be null.
     *                           Must be &lt;= <code>Integer.MAX_VALUE</code> ms.
     * @param maxNumberOfRetries The maximum number of retries. Must be a non-negative number.
     * @param maxDelayMs maximum value that {@code initialDelay} may grow to - once hit the delay is constant until {@code}maxNumberOfRetries is reached.
     * @return A backoff policy with an exponential increase in wait time for retries. The returned instance is thread safe but each
     * iterator created from it should only be used by a single thread.
     */
    public static Iterable<TimeValue> exponentialBackoff(int initialDelayMs, int maxNumberOfRetries, int maxDelayMs) {
        return () -> LongStream.iterate(1, i -> i + 1)
            .mapToObj(i -> TimeValue.timeValueMillis(Math.min(exp(initialDelayMs, i), maxDelayMs)))
            .limit(maxNumberOfRetries)
            .iterator();
    }
}
