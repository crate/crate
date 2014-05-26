/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect;

import io.crate.core.collections.NonBlockingArrayQueue;
import io.crate.core.collections.NoopQueue;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stats tables that are globally available on each node and contain meta data of the cluster
 * like active jobs
 *
 * injected via guice instead of using static so that if two nodes run
 * in the same jvm the memoryTables aren't shared between the nodes.
 */
public class StatsTables {

    public final Map<UUID, JobContext> jobsTable;
    private AtomicReference<BlockingQueue<JobContextLog>> jobsLog;
    private AtomicReference<Integer> lastLogSize;

    @Inject
    public StatsTables(Settings settings, NodeSettingsService nodeSettingsService) {
        jobsTable = new ConcurrentHashMap<>();
        lastLogSize = new AtomicReference<>(
                settings.getAsInt(ClusterSettingsExpression.SETTING_JOBS_LOG_SIZE, 0));

        int logSize = lastLogSize.get();
        if (logSize == 0) {
            jobsLog = new AtomicReference<>((BlockingQueue<JobContextLog>)new NoopQueue<JobContextLog>());
        } else {
            jobsLog = new AtomicReference<>((BlockingQueue<JobContextLog>)new NonBlockingArrayQueue<JobContextLog>(logSize));
        }
        nodeSettingsService.addListener(new NodeSettingsService.Listener() {
            @Override
            public void onRefreshSettings(Settings settings) {
                int newLogSize = settings.getAsInt(ClusterSettingsExpression.SETTING_JOBS_LOG_SIZE, 0);
                if (newLogSize != lastLogSize.get()) {
                    lastLogSize.set(newLogSize);
                    if (newLogSize == 0) {
                        jobsLog.set(new NoopQueue<JobContextLog>());
                    } else {
                        jobsLog.set(new NonBlockingArrayQueue<JobContextLog>(newLogSize));
                    }
                }
            }
        });
    }

    /**
     * returns a queue that implements the BlockingQueue interface which contains the current
     * JobContext log
     *
     * NOTE: The queue isn't actually a blocking queue but instead the implementation makes sure that
     * offer() never blocks.
     *
     * This is done because there is no Unblocking/Bounding queue available in java.
     */
    public BlockingQueue<JobContextLog> jobsLog() {
        return jobsLog.get();
    }
}