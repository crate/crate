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
import org.elasticsearch.search.profile.ProfileResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Simple stop watch type class that can be used as a context across multiple layers (analyzer, planner, executor)
 * to accumulate timing results in a map.
 *
 * It is not meant to be thread-safe.
 *
 */
public class ProfilingContext {

    private static final double NS_TO_MS_FACTOR = 1_000_000.0d;
    private final ImmutableMap.Builder<String, Double> durationInMSByTimer;
    private final Supplier<List<ProfileResult>> queryProfilingResults;

    public ProfilingContext(Supplier<List<ProfileResult>> queryProfilingResults) {
        this.queryProfilingResults = queryProfilingResults;
        this.durationInMSByTimer = ImmutableMap.builder();
    }

    public Map<String, Object> getDurationInMSByTimer() {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.putAll(durationInMSByTimer.build());
        ArrayList<Map<String, Object>> queryTimings = new ArrayList<>();
        for (ProfileResult profileResult : queryProfilingResults.get()) {
            queryTimings.add(resultAsMap(profileResult));
        }
        if (!queryTimings.isEmpty()) {
            builder.put("QueryBreakdown", queryTimings);
        }
        return builder.build();
    }

    private static Map<String, Object> resultAsMap(ProfileResult profileResult) {
        ImmutableMap.Builder<String, Object> queryTimingsBuilder = ImmutableMap.<String, Object>builder()
            .put("QueryName", profileResult.getQueryName())
            .put("QueryDescription", profileResult.getLuceneDescription())
            .put("Time", profileResult.getTime() / NS_TO_MS_FACTOR)
            .put("BreakDown", profileResult.getTimeBreakdown().entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getKey().endsWith("_count") ? e.getValue() : e.getValue() / NS_TO_MS_FACTOR))
            );
        List<Map<String, Object>> children = profileResult.getProfiledChildren().stream()
            .map(ProfilingContext::resultAsMap)
            .collect(Collectors.toList());
        if (!children.isEmpty()) {
            queryTimingsBuilder.put("Children", children);
        }
        return queryTimingsBuilder.build();
    }

    public Timer createAndStartTimer(String name) {
        Timer timer = createTimer(name);
        timer.start();
        return timer;
    }

    public void stopTimerAndStoreDuration(Timer timer) {
        timer.stop();
        durationInMSByTimer.put(timer.name(), timer.durationNanos() / NS_TO_MS_FACTOR);
    }

    public Timer createTimer(String name) {
        return new Timer(name);
    }

    public static String generateProfilingKey(int id, String name) {
        return id + "-" + name;
    }
}
