/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGenerations;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

/**
 * Holds information about currently in-flight shard level snapshot or clone operations on a per-shard level.
 * Concretely, this means information on which shards are actively being written to in the repository currently
 * as well as the latest written shard generation per shard in case there is a shard generation for a shard that has
 * been cleanly written out to the repository but not yet made part of the current {@link org.elasticsearch.repositories.RepositoryData}
 * through a snapshot finalization.
 */
public final class InFlightShardSnapshotStates {

    /**
     * Compute information about all shard ids that currently have in-flight state for the given repository.
     *
     * @param repoName  repository name
     * @param snapshots snapshots in progress
     * @return in flight shard states for all snapshot operation running for the given repository name
     */
    public static InFlightShardSnapshotStates forRepo(String repoName, List<SnapshotsInProgress.Entry> snapshots) {
        final Map<String, Map<Integer, String>> generations = new HashMap<>();
        final Map<String, Set<Integer>> busyIds = new HashMap<>();
        for (SnapshotsInProgress.Entry runningSnapshot : snapshots) {
            if (runningSnapshot.repository().equals(repoName) == false) {
                continue;
            }
            for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shard : runningSnapshot.shards()) {
                final ShardId sid = shard.key;
                addStateInformation(generations, busyIds, shard.value, sid.id(), sid.getIndexName());
            }
        }
        return new InFlightShardSnapshotStates(generations, busyIds);
    }

    private static void addStateInformation(Map<String, Map<Integer, String>> generations, Map<String, Set<Integer>> busyIds,
                                            SnapshotsInProgress.ShardSnapshotStatus shardState, int shardId, String indexName) {
        if (shardState.isActive()) {
            busyIds.computeIfAbsent(indexName, k -> new HashSet<>()).add(shardId);
            assert assertGenerationConsistency(generations, indexName, shardId, shardState.generation());
        } else if (shardState.state() == SnapshotsInProgress.ShardState.SUCCESS) {
            assert busyIds.getOrDefault(indexName, Collections.emptySet()).contains(shardId) == false :
                "Can't have a successful operation queued after an in-progress operation";
            generations.computeIfAbsent(indexName, k -> new HashMap<>()).put(shardId, shardState.generation());
        }
    }

    /**
     * Map that maps index name to a nested map of shard id to most recent successful shard generation for that
     * shard id.
     */
    private final Map<String, Map<Integer, String>> generations;

    /**
     * Map of index name to a set of shard ids that currently are actively executing an operation on the repository.
     */
    private final Map<String, Set<Integer>> activeShardIds;


    private InFlightShardSnapshotStates(Map<String, Map<Integer, String>> generations, Map<String, Set<Integer>> activeShardIds) {
        this.generations = generations;
        this.activeShardIds = activeShardIds;
    }

    private static boolean assertGenerationConsistency(Map<String, Map<Integer, String>> generations, String indexName,
                                                       int shardId, @Nullable String activeGeneration) {
        final String bestGeneration = generations.getOrDefault(indexName, Collections.emptyMap()).get(shardId);
        assert bestGeneration == null || activeGeneration == null || activeGeneration.equals(bestGeneration);
        return true;
    }

    /**
     * Check if a given shard currently has an actively executing shard operation.
     *
     * @param indexName name of the shard's index
     * @param shardId   shard id of the shard
     * @return true if shard has an actively executing shard operation
     */
    boolean isActive(String indexName, int shardId) {
        return activeShardIds.getOrDefault(indexName, Collections.emptySet()).contains(shardId);
    }

    /**
     * Determine the current generation for a shard by first checking if any in-flight but successful new shard
     * snapshots or clones have set a relevant generation and then falling back to {@link ShardGenerations#getShardGen}
     * if not.
     *
     * @param indexId          index id of the shard
     * @param shardId          shard id of the shard
     * @param shardGenerations current shard generations in the repository data
     * @return most recent shard generation for the given shard
     */
    @Nullable
    String generationForShard(IndexId indexId, int shardId, ShardGenerations shardGenerations) {
        final String inFlightBest = generations.getOrDefault(indexId.getName(), Collections.emptyMap()).get(shardId);
        if (inFlightBest != null) {
            return inFlightBest;
        }
        return shardGenerations.getShardGen(indexId, shardId);
    }
}
