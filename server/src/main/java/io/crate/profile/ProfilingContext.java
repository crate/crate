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

package io.crate.profile;

import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final HashMap<String, Double> durationInMSByTimer;
    private final List<QueryProfiler> profilers;

    public ProfilingContext(List<QueryProfiler> profilers) {
        this.profilers = profilers;
        this.durationInMSByTimer = new HashMap<>();
    }

    public Map<String, Object> getDurationInMSByTimer() {
        HashMap<String, Object> builder = new HashMap<>(durationInMSByTimer);
        ArrayList<Map<String, Object>> queryTimings = new ArrayList<>();
        for (var profiler : profilers) {
            for (var profileResult : profiler.getTree()) {
                queryTimings.add(resultAsMap(profileResult));
            }
        }
        if (!queryTimings.isEmpty()) {
            builder.put("QueryBreakdown", queryTimings);
        }
        return Collections.unmodifiableMap(builder);
    }

    private static Map<String, Object> resultAsMap(ProfileResult profileResult) {
        HashMap<String, Object> queryTimingsBuilder = new HashMap<>();
        queryTimingsBuilder.put("QueryName", profileResult.getQueryName());
        queryTimingsBuilder.put("QueryDescription", profileResult.getLuceneDescription());
        queryTimingsBuilder.put("Time", profileResult.getTime() / NS_TO_MS_FACTOR);
        queryTimingsBuilder.put("BreakDown", profileResult.getTimeBreakdown().entrySet().stream()
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
        return Collections.unmodifiableMap(queryTimingsBuilder);
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
