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
import java.util.function.BiFunction;

/**
 * Counts how many operations issued from the current node are in progress across the cluster.
 * If the destination node cannot be determined, it counts the in progress operations towards the unknown node.
 * Note: one job can span multiple nodes.
 */
@Singleton
public class NodeJobsCounter {

    /**
     * Represents the maximum number of concurrent operations that can be issued towards a node.
     */
    public static final long MAX_NODE_CONCURRENT_OPERATIONS = 5;

    private long unknownNodeCount = 0L;
    // Using single element long[] to avoid autoboxing
    private final Map<String, long[]> operationsCountPerNode = new ConcurrentHashMap<>();

    private static BiFunction<String, long[], long[]> INCREMENT_COUNTER_FOR_NODE = (node, count) -> {
        if (count == null) {
            count = new long[1];
            count[0] = 1;
        } else {
            count[0]++;
        }
        return count;
    };

    private static BiFunction<String, long[], long[]> DECREMENT_COUNTER_FOR_NODE = (id, count) -> {
        if (count == null) {
            count = new long[1];
            count[0] = 0;
        } else {
            count[0]--;
        }
        return count;
    };


    public void increment(@Nullable String nodeId) {
        if (nodeId == null) {
            unknownNodeCount++;
        } else {
            operationsCountPerNode.compute(nodeId, INCREMENT_COUNTER_FOR_NODE);
        }
    }

    public void decrement(@Nullable String nodeId) {
        if (nodeId == null) {
            unknownNodeCount--;
        } else {
            operationsCountPerNode.compute(nodeId, DECREMENT_COUNTER_FOR_NODE);
        }
    }

    public long getInProgressJobsForNode(@Nullable String nodeId) {
        long count;
        if (nodeId == null) {
            count = unknownNodeCount;
        } else {
            long[] countPerNode = operationsCountPerNode.get(nodeId);
            count = countPerNode == null ? 0L : countPerNode[0];
        }
        return count;
    }
}
