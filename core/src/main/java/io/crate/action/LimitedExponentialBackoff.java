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

package io.crate.action;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Iterator;
import java.util.NoSuchElementException;


public class LimitedExponentialBackoff extends BackoffPolicy {

    private final int startValue;
    private final int maxIterations;
    private final int limit;

    public LimitedExponentialBackoff(int startValue, int maxIterations, int limit) {
        assert startValue >= 0 : "startValue should be >= 0";
        assert maxIterations > 0 : "maxIterations should be > 0";
        assert limit > 0 : "limit should be > 0";
        this.startValue = startValue;
        this.maxIterations = maxIterations;
        this.limit = limit;
    }

    public static BackoffPolicy limitedExponential(int limit) {
        return new LimitedExponentialBackoff(0, Integer.MAX_VALUE, limit);
    }

    private static class LimitedExponentialBackoffIterator implements Iterator<TimeValue> {

        private static final float FACTOR = 1.8f;

        private final int startValue;
        private final int maxIterations;
        private final int limit;
        private int currentIterations;

        private LimitedExponentialBackoffIterator(int startValue, int maxIterations, int limit) {
            this.startValue = startValue;
            this.maxIterations = maxIterations;
            this.limit = limit;
        }

        private int calculate(int iteration) {
            return Math.min(limit, (int) Math.pow(iteration / Math.E, FACTOR));
        }

        @Override
        public boolean hasNext() {
            return currentIterations < maxIterations;
        }

        @Override
        public TimeValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException(
                    "Reached maximum amount of backoff iterations. Only " + maxIterations + " iterations allowed.");
            }
            int result = startValue + calculate(currentIterations);
            currentIterations++;
            return TimeValue.timeValueMillis(result);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported for " +
                                                    LimitedExponentialBackoff.class.getSimpleName());
        }

    }

    @Override
    public Iterator<TimeValue> iterator() {
        return new LimitedExponentialBackoffIterator(startValue, maxIterations, limit);
    }
}
