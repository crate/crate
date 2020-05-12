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

import io.crate.common.unit.TimeValue;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a backoff policy for bulk requests. Whenever a bulk request is rejected due to resource constraints (i.e. the client's internal
 * thread pool is full), the backoff policy decides how long the bulk processor will wait before the operation is retried internally.
 *
 * Notes for implementing custom subclasses:
 *
 * The underlying mathematical principle of <code>BackoffPolicy</code> are progressions which can be either finite or infinite although
 * the latter should not be used for retrying. A progression can be mapped to a <code>java.util.Iterator</code> with the following
 * semantics:
 *
 * <ul>
 *     <li><code>#hasNext()</code> determines whether the progression has more elements. Return <code>true</code> for infinite progressions</li>
 *     <li><code>#next()</code> determines the next element in the progression, i.e. the next wait time period</li>
 * </ul>
 *
 * Note that backoff policies are exposed as <code>Iterables</code> in order to be consumed multiple times.
 */
public abstract class BackoffPolicy implements Iterable<TimeValue> {

    /**
     * Creates an new exponential backoff policy with a default configuration of 50 ms initial wait period and 8 retries taking
     * roughly 5.1 seconds in total.
     *
     * @return A backoff policy with an exponential increase in wait time for retries. The returned instance is thread safe but each
     * iterator created from it should only be used by a single thread.
     */
    public static BackoffPolicy exponentialBackoff() {
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
    public static BackoffPolicy exponentialBackoff(TimeValue initialDelay, int maxNumberOfRetries) {
        return new ExponentialBackoff((int) checkDelay(initialDelay).millis(), maxNumberOfRetries);
    }

    private static TimeValue checkDelay(TimeValue delay) {
        if (delay.millis() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("delay must be <= " + Integer.MAX_VALUE + " ms");
        }
        return delay;
    }

    private static class ExponentialBackoff extends BackoffPolicy {
        private final int start;

        private final int numberOfElements;

        private ExponentialBackoff(int start, int numberOfElements) {
            assert start >= 0;
            assert numberOfElements >= 0;
            this.start = start;
            this.numberOfElements = numberOfElements;
        }

        @Override
        public Iterator<TimeValue> iterator() {
            return new ExponentialBackoffIterator(start, numberOfElements);
        }
    }

    private static class ExponentialBackoffIterator implements Iterator<TimeValue> {
        private final int numberOfElements;

        private final int start;

        private int currentlyConsumed;

        private ExponentialBackoffIterator(int start, int numberOfElements) {
            this.start = start;
            this.numberOfElements = numberOfElements;
        }

        @Override
        public boolean hasNext() {
            return currentlyConsumed < numberOfElements;
        }

        @Override
        public TimeValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Only up to " + numberOfElements + " elements");
            }
            int result = start + 10 * ((int) Math.exp(0.8d * (currentlyConsumed)) - 1);
            currentlyConsumed++;
            return TimeValue.timeValueMillis(result);
        }
    }
}
