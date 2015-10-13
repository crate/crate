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

package io.crate.jobs;

import io.crate.executor.transport.TransportActionProvider;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Singleton
public class KillJobsReaper implements Reaper {

    private static final ESLogger LOGGER = Loggers.getLogger(KillJobsReaper.class);
    private final ThreadPool threadPool;
    private final TransportActionProvider transportActionProvider;

    @Inject
    public KillJobsReaper(ThreadPool threadPool, TransportActionProvider transportActionProvider) {
        this.threadPool = threadPool;
        this.transportActionProvider = transportActionProvider;
    }

    @Override
    public void killHangingJobs(TimeValue maxKeepAliveTime, Collection<JobExecutionContext> contexts) {
        final Set<UUID> jobsToKill = new HashSet<>();

        final long time = threadPool.estimatedTimeInMillis();
        for (JobExecutionContext context : contexts) {
            // Use the same value for both checks since lastAccessTime can
            // be modified by another thread between checks!
            final long lastAccessTime = context.lastAccessTime();
            if (lastAccessTime == -1L) { // its being processed or timeout is disabled
                continue;
            }
            if ((time - lastAccessTime > maxKeepAliveTime.getMillis())) {
                UUID id = context.jobId();
                LOGGER.debug("closing job collect context [{}], time [{}], lastAccessTime [{}]",
                        id, time, lastAccessTime);
                jobsToKill.add(id);
            }
        }
        if (!jobsToKill.isEmpty()) {
            PlainListenableActionFuture<KillResponse> future = new PlainListenableActionFuture<>(false, threadPool);
            transportActionProvider.transportKillJobsNodeAction().executeKillOnAllNodes(new KillJobsRequest(jobsToKill), future);
            try {
                KillResponse response = future.actionGet(TimeValue.timeValueSeconds(30));
                LOGGER.debug("killed {} jobs: {}", response.numKilled(), jobsToKill);
            } catch (Throwable t) {
                LOGGER.warn("An error occurred while killing jobs {}", t, jobsToKill);
            }
        }
    }
}
