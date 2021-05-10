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

package io.crate.execution.engine.collect.collectors;

import io.crate.breaker.RamAccounting;
import io.crate.common.annotations.VisibleForTesting;
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
import io.crate.metadata.settings.SessionSettings;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
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
    private final RamAccounting ramAccounting;
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
                           RamAccounting ramAccounting,
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
        this.ramAccounting = ramAccounting;
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
                context.kill(t.getMessage());
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
                context.kill(null);
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
                new ActionListener<>() {
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
                        context.kill(e.getMessage());
                    }
                }
            );
        }
    }

    private RootTask.Builder createPageDownstreamContext() {
        RootTask.Builder builder = tasksService.newBuilder(
            jobId,
            sessionSettings.userName(),
            localNode,
            Collections.emptySet()
        );

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
            ramAccounting,
            1
        ));
        return builder;
    }

    private void killRemoteContext() {
        KillJobsRequest killRequest = new KillJobsRequest(
            List.of(jobId),
            sessionSettings.userName(),
            null
        );
        transportKillJobsNodeAction.broadcast(killRequest, new ActionListener<>() {

                @Override
                public void onResponse(Long numKilled) {
                    context.kill(null);
                }

                @Override
                public void onFailure(Exception e) {
                    context.kill(e.getMessage());
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
