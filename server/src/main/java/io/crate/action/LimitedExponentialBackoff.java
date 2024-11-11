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

package io.crate.action;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.elasticsearch.action.bulk.BackoffPolicy;

import io.crate.common.unit.TimeValue;


public class LimitedExponentialBackoff extends BackoffPolicy {

    private final int firstDelayInMS;
    private final int maxIterations;
    private final int maxDelayInMS;

    public LimitedExponentialBackoff(int firstDelayInMS, int maxIterations, int maxDelayInMS) {
        assert firstDelayInMS >= 0 : "firstDelayInMS should be >= 0";
        assert maxIterations > 0 : "maxIterations should be > 0";
        assert maxDelayInMS > 0 : "maxDelayInMS should be > 0";
        this.firstDelayInMS = firstDelayInMS;
        this.maxIterations = maxIterations;
        this.maxDelayInMS = maxDelayInMS;
    }

    private static class LimitedExponentialBackoffIterator implements Iterator<TimeValue> {

        private static final float FACTOR = 1.8f;

        private final int firstDelayInMS;
        private final int maxIterations;
        private final int mayDelayInMS;
        private int currentIterations = 0;

        private LimitedExponentialBackoffIterator(int firstDelayInMS, int maxIterations, int maxDelayInMs) {
            this.firstDelayInMS = firstDelayInMS;
            this.maxIterations = maxIterations;
            this.mayDelayInMS = maxDelayInMs;
        }

        private int calculate(int iteration) {
            return Math.min(mayDelayInMS, (int) Math.pow(iteration / Math.E, FACTOR));
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
            int result = firstDelayInMS + calculate(currentIterations);
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
        return new LimitedExponentialBackoffIterator(firstDelayInMS, maxIterations, maxDelayInMS);
    }
}
