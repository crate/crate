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

package io.crate.operation;

import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Counts how many operations issued from the current node are in progress across the cluster.
 * If the destination node cannot be determined, it counts the in progress operations towards the unknown node.
 * Note: one job can span multiple nodes.
 */
@Singleton
public class NodeJobsCounter {

    /**
     * Represents the maximum number of concurrent jobs that can be issued towards a node.
     */
    public static final long MAX_NODE_CONCURRENT_JOBS = 5;

    /**
     * Represents the maximum number of parked retry operations that can be outstanding against a node.
     */
    public static final long MAX_NODE_PARKED_RETRIES = 1;

    private long unknownNodeJobsCount = 0L;
    private long unknownNodeRetriesCount = 0L;
    private final Map<String, long[]> jobsCountPerNode = new ConcurrentHashMap<>();
    private final Map<String, long[]> retriesCountPerNode = new ConcurrentHashMap<>();

    public void incJobsCount(@Nullable String nodeId) {
        if (nodeId == null) {
            unknownNodeJobsCount++;
        } else {
            increment(nodeId, jobsCountPerNode);
        }
    }

    public void incRetriesCount(@Nullable String nodeId) {
        if (nodeId == null) {
            unknownNodeRetriesCount++;
        } else {
            increment(nodeId, retriesCountPerNode);
        }
    }

    private void increment(String nodeId, Map<String, long[]> nodeOperations) {

        nodeOperations.compute(nodeId, (node, count) -> {
                if (count == null) {
                    count = new long[1];
                    count[0] = 1;
                } else {
                    count[0]++;
                }
                return count;
            }
        );
    }

    public void decJobsCount(@Nullable String nodeId) {
        if (nodeId == null) {
            unknownNodeJobsCount--;
        } else {
            decrement(nodeId, jobsCountPerNode);
        }
    }

    public void decRetriesCount(@Nullable String nodeId) {
        if (nodeId == null) {
            unknownNodeRetriesCount--;
        } else {
            decrement(nodeId, retriesCountPerNode);
        }
    }

    private void decrement(String nodeId, Map<String, long[]> retriesCountPerNode) {
        retriesCountPerNode.compute(nodeId, (id, count) -> {
            if (count == null) {
                count = new long[1];
                count[0] = 0;
            } else {
                count[0]--;
            }
            return count;
        });
    }

    public long getInProgressJobsForNode(@Nullable String nodeId) {
        if (nodeId == null) {
            return unknownNodeJobsCount;
        } else {
            long[] countPerNode = jobsCountPerNode.get(nodeId);
            return countPerNode == null ? 0L : countPerNode[0];
        }
    }

    public long getParkedRetriesForNode(@Nullable String nodeId) {
        if (nodeId == null) {
            return unknownNodeRetriesCount;
        } else {
            long[] countPerNode = retriesCountPerNode.get(nodeId);
            return countPerNode == null ? 0L : countPerNode[0];
        }
    }
}
