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

package io.crate.metadata.sys;

import io.crate.planner.operators.StatementClassifier.Classification;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ClassifiedHistograms implements Iterable<ClassifiedHistograms.ClassifiedHistogram> {

    private static final long HIGHEST_TRACKABLE_VALUE = TimeUnit.MINUTES.toMillis(10);
    private static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 3;

    private final ConcurrentHashMap<Classification, ConcurrentHistogram> histograms = new ConcurrentHashMap<>();

    public static class ClassifiedHistogram {

        private Histogram histogram;
        private Classification classification;

        ClassifiedHistogram(Histogram histogram, Classification classification) {
            this.histogram = histogram;
            this.classification = classification;
        }

        public Histogram histogram() {
            return histogram;
        }

        public Classification classification() {
            return classification;
        }
    }

    public void recordValue(Classification classification, long duration) {
        // We use start and end time to calculate the duration (since we track them anyway)
        // If the system time is adjusted this can lead to negative durations
        // so we protect here against it.
        getOrCreate(classification).recordValue(Math.min(Math.max(0, duration), HIGHEST_TRACKABLE_VALUE));
    }

    private ConcurrentHistogram getOrCreate(Classification classification) {
        ConcurrentHistogram histogram = histograms.get(classification);
        if (histogram == null) {
            histogram = new ConcurrentHistogram(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
            histograms.put(classification, histogram);
        }
        return histogram;
    }

    public void reset() {
        histograms.clear();
    }

    @Override
    public Iterator<ClassifiedHistogram> iterator() {
        return histograms.entrySet()
            .stream()
            .map(e -> new ClassifiedHistogram(e.getValue().copy(), e.getKey()))
            .iterator();
    }
}
