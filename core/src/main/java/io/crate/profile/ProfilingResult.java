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

import org.elasticsearch.search.profile.ProfileResult;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// TODO add serialization
public class ProfilingResult {

    private final String label;

    @Nullable
    private Map<String, String> metadata;
    @Nullable
    private final Map<String, Long> timings;
    @Nullable
    private List<ProfilingResult> childrenResults;

    public ProfilingResult(String label) {
        this(label, null, null, null);
    }

    public ProfilingResult(String label, @Nullable Map<String, Long> timings, @Nullable Map<String, String> metadata,
                           @Nullable List<ProfilingResult> childrenResults) {
        this.label = label;
        this.timings = timings;
        this.metadata = metadata;
        this.childrenResults = childrenResults;
    }

    public String getLabel() {
        return label;
    }

    public long getTotalTime() {
        return sumTimings(timings);
    }

    @Nullable
    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void addMetadata(String key, String value) {
        if (metadata == null) {
            metadata = new HashMap<>();
        }
        metadata.put(key, value);
    }

    @Nullable
    public Map<String, Long> getTimings() {
        return Collections.unmodifiableMap(timings);
    }

    @Nullable
    public List<ProfilingResult> getChildrenResults() {
        return childrenResults;
    }

    public void addChildResult(ProfilingResult childResult) {
        if (childrenResults == null) {
            childrenResults = new ArrayList<>();
        }

        childrenResults.add(childResult);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProfilingResult that = (ProfilingResult) o;
        return Objects.equals(label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label);
    }

    private static long sumTimings(@Nullable Map<String, Long> timings) {
        if (timings == null) {
            return 0;
        }

        long sum = 0;
        for (Long time : timings.values()) {
            sum += time;
        }
        return sum;
    }

    // TODO: remove this
    @Override
    public String toString() {
        return "ProfilingResult{" +
               "label='" + label + '\'' +
               ", metadata=" + metadata +
               ", timings=" + timings +
               ", totalTime=" + getTotalTime() +
               ", childrenResults=" + childrenResults +
               '}';
    }

    public static ProfilingResult fromProfileResult(ProfileResult profileResult) {
        Map<String, String> metadata = new HashMap<>(1);
        metadata.put("type", profileResult.getQueryName());
        Map<String, Long> timeBreakdownNanos = profileResult.getTimeBreakdown();
        Map<String, Long> timeBreakdownMillis = new HashMap<>(timeBreakdownNanos.size());
        timeBreakdownNanos.forEach((k, v) -> timeBreakdownMillis.put(k, v/1_000_000L));
        ProfilingResult convertedResult = new ProfilingResult(profileResult.getLuceneDescription(),
            timeBreakdownMillis, metadata, null);

        if (profileResult.getProfiledChildren() != null) {
            for (ProfileResult child : profileResult.getProfiledChildren()) {
                convertedResult.addChildResult(fromProfileResult(child));
            }
        }

        return convertedResult;
    }
}
