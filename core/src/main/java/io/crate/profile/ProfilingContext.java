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

package io.crate.profile;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.search.profile.query.QueryProfiler;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Simple stop watch type class that can be used as a context across multiple layers (analyzer, planner, executor)
 * to accumulate timing results in a map.
 * <p>
 * It is not meant to be thread-safe.
 */
public class ProfilingContext {

    private final boolean enabled;
    private ImmutableMap.Builder<String, Long> map;
    private final Function<String, TimeMeasurable> measurableFactory;
    private Map<String, ProfilingResult> results = new ConcurrentHashMap<>();
    @Nullable
    private final QueryProfiler queryProfiler;

    public ProfilingContext(boolean enabled) {
        this.enabled = enabled;
        this.map = ImmutableMap.builder();
        this.measurableFactory = enabled ? InternalTimeMeasurable::new : NoopTimeMeasurable::new;
        this.queryProfiler = enabled ? new QueryProfiler() : null;
    }

    public boolean enabled() {
        return enabled;
    }

    public TimeMeasurable createAndStartMeasurable(String name) {
        TimeMeasurable timer = createMeasurable(name);
        timer.start();
        return timer;
    }

    public void stopAndAddMeasurable(TimeMeasurable measurable) {
        measurable.stop();
        if (enabled) {
            map.put(measurable.name(), measurable.durationNanos() / 1_000_000L);
        }
    }

    /**
     * Materialize the accumulated timings into a {@link ProfilingResult} for the provided label.
     * If there already is a {@link ProfilingResult} in the {@link ProfilingContext} the existing result will be
     * returned.
     */
    public ProfilingResult materializeResultFromCurrentTimings(String label) {
        ProfilingResult result = results.get(label);
        if (result != null) {
            return result;
        }
        ProfilingResult profilingResult = new ProfilingResult(label, map.build(), null, null);
        results.put(label, profilingResult);
        map = ImmutableMap.builder();
        return profilingResult;
    }

    public ProfilingResult getResult(String label) {
        return results.get(label);
    }

    public Collection<ProfilingResult> getResults() {
        return Collections.unmodifiableCollection(results.values());
    }

    public TimeMeasurable createMeasurable(String name) {
        return measurableFactory.apply(name);
    }

    // TODO maybe the QueryProfiler can be a TimeMeasurable?
    @Nullable
    public QueryProfiler queryProfiler() {
        return queryProfiler;
    }
}
