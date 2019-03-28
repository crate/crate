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

package io.crate.execution.engine.collect.collectors;

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.settings.SessionSettings;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.distribution.merge.PassThroughPagingIterator;
import io.crate.execution.jobs.CumulativePageBucketReceiver;
import io.crate.execution.jobs.DistResultRXTask;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsRequest;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.transport.JobRequest;
import io.crate.execution.jobs.transport.JobResponse;
import io.crate.execution.jobs.transport.TransportJobAction;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executor;

public class RemoteCollector {

    private static final Logger LOGGER = LogManager.getLogger(RemoteCollector.class);
    private static final int RECEIVER_PHASE_ID = 1;

    private final UUID jobId;
    private final SessionSettings sessionSettings;
    private final String localNode;
    private final String remoteNode;
    private final Executor executor;
    private final TransportJobAction transportJobAction;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final TasksService tasksService;
    private final RamAccountingContext ramAccountingContext;
    private final RowConsumer consumer;
    private final RoutedCollectPhase collectPhase;

    private final Object killLock = new Object();
    private final boolean scrollRequired;
    private final boolean enableProfiling;
    private RootTask context = null;
    private boolean collectorKilled = false;

    public RemoteCollector(UUID jobId,
                           SessionSettings sessionSettings,
                           String localNode,
                           String remoteNode,
                           TransportJobAction transportJobAction,
                           TransportKillJobsNodeAction transportKillJobsNodeAction,
                           Executor executor,
                           TasksService tasksService,
                           RamAccountingContext ramAccountingContext,
                           RowConsumer consumer,
                           RoutedCollectPhase collectPhase) {
        this.jobId = jobId;
        this.sessionSettings = sessionSettings;
        this.localNode = localNode;
        this.remoteNode = remoteNode;
        this.executor = executor;

        /*
         * We don't wanna profile the timings of the remote execution context, because the remoteCollect is already
         * part of the subcontext duration of the original Task profiling.
         */
        this.enableProfiling = false;

        this.scrollRequired = consumer.requiresScroll();
        this.transportJobAction = transportJobAction;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        this.tasksService = tasksService;
        this.ramAccountingContext = ramAccountingContext;
        this.consumer = consumer;
        this.collectPhase = collectPhase;
    }

    public void doCollect() {
        if (!createLocalContext()) return;
        createRemoteContext();
    }

    @VisibleForTesting
    boolean createLocalContext() {
        RootTask.Builder builder = createPageDownstreamContext();
        try {
            synchronized (killLock) {
                if (collectorKilled) {
                    consumer.accept(null, new InterruptedException());
                    return false;
                }
                context = tasksService.createTask(builder);
                context.start();
                return true;
            }
        } catch (Throwable t) {
            if (context == null) {
                consumer.accept(null, t);
            } else {
                context.kill();
            }
            return false;
        }
    }

    @VisibleForTesting
    void createRemoteContext() {
        NodeOperation nodeOperation = new NodeOperation(
            collectPhase, Collections.singletonList(localNode), RECEIVER_PHASE_ID, (byte) 0);

        synchronized (killLock) {
            if (collectorKilled) {
                context.kill();
                return;
            }
            transportJobAction.execute(
                remoteNode,
                new JobRequest(
                    jobId,
                    sessionSettings,
                    localNode,
                    Collections.singletonList(nodeOperation),
                    enableProfiling
                ),
                new ActionListener<JobResponse>() {
                    @Override
                    public void onResponse(JobResponse jobResponse) {
                        LOGGER.trace("RemoteCollector jobAction=onResponse");
                        if (collectorKilled) {
                            killRemoteContext();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.error("RemoteCollector jobAction=onFailure", e);
                        context.kill();
                    }
                }
            );
        }
    }

    private RootTask.Builder createPageDownstreamContext() {
        RootTask.Builder builder = tasksService.newBuilder(jobId, localNode, Collections.emptySet());

        PassThroughPagingIterator<Integer, Row> pagingIterator;
        if (scrollRequired) {
            pagingIterator = PassThroughPagingIterator.repeatable();
        } else {
            pagingIterator = PassThroughPagingIterator.oneShot();
        }
        PageBucketReceiver pageBucketReceiver = new CumulativePageBucketReceiver(
            localNode,
            RECEIVER_PHASE_ID,
            executor,
            DataTypes.getStreamers(collectPhase.outputTypes()),
            consumer,
            pagingIterator,
            1);

        builder.addTask(new DistResultRXTask(
            RECEIVER_PHASE_ID,
            "RemoteCollectPhase",
            pageBucketReceiver,
            ramAccountingContext,
            1
        ));
        return builder;
    }

    private void killRemoteContext() {
        transportKillJobsNodeAction.broadcast(new KillJobsRequest(Collections.singletonList(jobId)),
            new ActionListener<Long>() {

                @Override
                public void onResponse(Long numKilled) {
                    context.kill();
                }

                @Override
                public void onFailure(Exception e) {
                    context.kill();
                }
            });
    }

    public void kill(@Nullable Throwable throwable) {
        synchronized (killLock) {
            collectorKilled = true;
            /**
             * due to the lock there are 3 kill windows:
             *
             *  1. localContext not even created - doCollect aborts
             *  2. localContext created, no requests sent - doCollect aborts
             *  3. localContext created, requests sent - clean-up happens once response from remote is received
             */
        }
    }
}
