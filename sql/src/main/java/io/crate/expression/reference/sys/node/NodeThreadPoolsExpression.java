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

package io.crate.expression.reference.sys.node;

import io.crate.expression.reference.sys.ArrayTypeNestableContextCollectExpression;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.HashMap;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;


public class NodeThreadPoolsExpression
    extends ArrayTypeNestableContextCollectExpression<NodeStatsContext, ThreadPoolStats.Stats, Object> {

    private static final String POOL_NAME = "name";
    private static final String ACTIVE = "active";
    private static final String REJECTED = "rejected";
    private static final String LARGEST = "largest";
    private static final String COMPLETED = "completed";
    private static final String THREADS = "threads";
    private static final String QUEUE = "queue";

    public NodeThreadPoolsExpression() {
    }

    @Override
    protected List<ThreadPoolStats.Stats> items(NodeStatsContext nodeStatsContext) {
        return newArrayList(nodeStatsContext.threadPools());
    }

    @Override
    protected Object valueForItem(ThreadPoolStats.Stats stats) {
        HashMap<String, Object> result = new HashMap<>(7);
        result.put(POOL_NAME, stats.getName());
        result.put(ACTIVE, stats.getActive());
        result.put(COMPLETED, stats.getCompleted());
        result.put(REJECTED, stats.getRejected());
        result.put(LARGEST, stats.getLargest());
        result.put(QUEUE, stats.getQueue());
        result.put(THREADS, stats.getThreads());
        return result;
    }

    @Override
    public void setNextRow(NodeStatsContext nodeStatsContext) {
        value = null;
        if (nodeStatsContext.isComplete()) {
            super.setNextRow(nodeStatsContext);
        }
    }
}
