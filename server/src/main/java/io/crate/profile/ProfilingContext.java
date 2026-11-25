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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.jspecify.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.metadata.PartitionName;

/**
 * Simple stop watch type class that can be used as a context across multiple layers (analyzer, planner, executor)
 * to accumulate timing results in a map.
 *
 * It is not meant to be thread-safe.
 *
 */
public class ProfilingContext {

    public record CountShardTiming(
        String schemaName,
        String tableName,
        @Nullable String partitionIdent,
        int shardId,
        String queryName,
        String queryDescription,
        double durationInMs
    ) {}

    private static final Comparator<CountShardTiming> COUNT_SHARD_TIMING_COMPARATOR = Comparator
        .comparing(CountShardTiming::schemaName)
        .thenComparing(CountShardTiming::tableName)
        .thenComparing(CountShardTiming::partitionIdent, Comparator.nullsFirst(Comparator.naturalOrder()))
        .thenComparingInt(CountShardTiming::shardId);

    private static final double NS_TO_MS_FACTOR = 1_000_000.0d;
    private final HashMap<String, Double> durationInMSByTimer;
    private final Map<ShardId, QueryProfiler> profilers;
    private final ClusterState currentState;
    private final ConcurrentLinkedQueue<CountShardTiming> countTimings;

    public ProfilingContext(Map<ShardId, QueryProfiler> profilers, ClusterState currentState) {
        this.profilers = profilers;
        this.durationInMSByTimer = new HashMap<>();
        this.currentState = currentState;
        this.countTimings = new ConcurrentLinkedQueue<>();
    }

    public void recordCountTiming(String schemaName,
                                  String tableName,
                                  @Nullable String partitionIdent,
                                  int shardId,
                                  String queryName,
                                  String queryDescription,
                                  long durationNanos) {
        recordCountTiming(
            schemaName,
            tableName,
            partitionIdent,
            shardId,
            queryName,
            queryDescription,
            durationNanos / NS_TO_MS_FACTOR
        );
    }

    public void recordCountTiming(String schemaName,
                                  String tableName,
                                  @Nullable String partitionIdent,
                                  int shardId,
                                  String queryName,
                                  String queryDescription,
                                  double durationInMs) {
        countTimings.add(new CountShardTiming(
            schemaName,
            tableName,
            partitionIdent,
            shardId,
            queryName,
            queryDescription,
            durationInMs
        ));
    }

    @VisibleForTesting
    public Collection<CountShardTiming> getCountTimings() {
        return Collections.unmodifiableCollection(countTimings);
    }

    public Map<String, Object> getDurationInMSByTimer() {
        HashMap<String, Object> builder = new HashMap<>(durationInMSByTimer);
        ArrayList<Map<String, Object>> queryTimings = new ArrayList<>();
        for (var entry : profilers.entrySet()) {
            ShardId shardId = entry.getKey();
            PartitionName partitionName = currentState.metadata().getPartitionName(shardId.getIndexUUID());
            var profiler = entry.getValue();
            for (var profileResult : profiler.getTree()) {
                queryTimings.add(resultAsMap(shardId, profileResult, partitionName));
            }
        }
        if (!countTimings.isEmpty()) {
            List<CountShardTiming> sortedCountTimings = new ArrayList<>(countTimings);
            sortedCountTimings.sort(COUNT_SHARD_TIMING_COMPARATOR);
            for (var timing : sortedCountTimings) {
                queryTimings.add(countResultAsMap(timing));
            }
        }
        if (!queryTimings.isEmpty()) {
            builder.put("QueryBreakdown", queryTimings);
        }
        return Collections.unmodifiableMap(builder);
    }

    private static Map<String, Object> resultAsMap(ShardId shardId,
                                                   ProfileResult profileResult,
                                                   PartitionName partitionName) {
        TreeMap<String, Object> queryTimingsBuilder = new TreeMap<>();
        queryTimingsBuilder.put("SchemaName", partitionName.relationName().schema());
        queryTimingsBuilder.put("TableName", partitionName.relationName().name());
        if (partitionName.values().isEmpty() == false) {
            queryTimingsBuilder.put("PartitionIdent", partitionName.ident());
        }
        queryTimingsBuilder.put("ShardId", shardId.id());
        queryTimingsBuilder.put("QueryName", profileResult.getQueryName());
        queryTimingsBuilder.put("QueryDescription", profileResult.getLuceneDescription());
        queryTimingsBuilder.put("Time", profileResult.getTime() / NS_TO_MS_FACTOR);
        queryTimingsBuilder.put("BreakDown", profileResult.getTimeBreakdown().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getKey().endsWith("_count") ? e.getValue() : e.getValue() / NS_TO_MS_FACTOR,
                (v1, ignored) -> v1,    // ignore duplicate keys, use the existing first one
                TreeMap::new
            ))
        );
        List<Map<String, Object>> children = profileResult.getProfiledChildren().stream()
            .map((ProfileResult pr) -> resultAsMap(shardId, pr, partitionName))
            .collect(Collectors.toList());
        if (!children.isEmpty()) {
            queryTimingsBuilder.put("Children", children);
        }
        return Collections.unmodifiableMap(queryTimingsBuilder);
    }

    private static Map<String, Object> countResultAsMap(CountShardTiming timing) {
        TreeMap<String, Object> countTimingsBuilder = new TreeMap<>();
        countTimingsBuilder.put("SchemaName", timing.schemaName());
        countTimingsBuilder.put("TableName", timing.tableName());
        if (timing.partitionIdent() != null && !timing.partitionIdent().isEmpty()) {
            countTimingsBuilder.put("PartitionIdent", timing.partitionIdent());
        }
        countTimingsBuilder.put("ShardId", timing.shardId());
        countTimingsBuilder.put("QueryName", timing.queryName());
        countTimingsBuilder.put("QueryDescription", timing.queryDescription());
        countTimingsBuilder.put("Time", timing.durationInMs());
        return Collections.unmodifiableMap(countTimingsBuilder);
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
