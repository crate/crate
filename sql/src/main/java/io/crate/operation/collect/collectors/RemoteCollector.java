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

package io.crate.operation.collect.collectors;

import com.google.common.annotations.VisibleForTesting;
import io.crate.action.job.JobRequest;
import io.crate.action.job.JobResponse;
import io.crate.action.job.TransportJobAction;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.PageDownstreamContext;
import io.crate.operation.NodeOperation;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.UUID;

public class RemoteCollector implements CrateCollector {

    private final static ESLogger LOGGER = Loggers.getLogger(RemoteCollector.class);
    private final static int RECEIVER_PHASE_ID = 1;

    private final UUID jobId;
    private final String localNode;
    private final String remoteNode;
    private final TransportJobAction transportJobAction;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final JobContextService jobContextService;
    private final RamAccountingContext ramAccountingContext;
    private final BatchConsumer consumer;
    private final RoutedCollectPhase collectPhase;

    private final Object killLock = new Object();
    private final boolean scrollRequired;
    private JobExecutionContext context = null;
    private boolean collectorKilled = false;

    public RemoteCollector(UUID jobId,
                           String localNode,
                           String remoteNode,
                           TransportJobAction transportJobAction,
                           TransportKillJobsNodeAction transportKillJobsNodeAction,
                           JobContextService jobContextService,
                           RamAccountingContext ramAccountingContext,
                           BatchConsumer consumer,
                           RoutedCollectPhase collectPhase) {
        this.jobId = jobId;
        this.localNode = localNode;
        this.remoteNode = remoteNode;

        this.scrollRequired = consumer.requiresScroll();
        this.transportJobAction = transportJobAction;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        this.jobContextService = jobContextService;
        this.ramAccountingContext = ramAccountingContext;
        this.consumer = consumer;
        this.collectPhase = collectPhase;
    }

    @Override
    public void doCollect() {
        if (!createLocalContext()) return;
        createRemoteContext();
    }

    @VisibleForTesting
    boolean createLocalContext() {
        JobExecutionContext.Builder builder = createPageDownstreamContext();
        try {
            synchronized (killLock) {
                if (collectorKilled) {
                    consumer.accept(null, new InterruptedException());
                    return false;
                }
                context = jobContextService.createContext(builder);
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
                new JobRequest(jobId, localNode, Collections.singletonList(nodeOperation)),
                new ActionListener<JobResponse>() {
                    @Override
                    public void onResponse(JobResponse jobResponse) {
                        LOGGER.trace("RemoteCollector jobAction=onResponse");
                        if (collectorKilled) {
                            killRemoteContext();
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.error("RemoteCollector jobAction=onFailure", e);
                        context.kill();
                    }
                }
            );
        }
    }

    private JobExecutionContext.Builder createPageDownstreamContext() {
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId, localNode);

        PassThroughPagingIterator<Integer, Row> pagingIterator;
        if (scrollRequired) {
            pagingIterator = PassThroughPagingIterator.repeatable();
        } else {
            pagingIterator = PassThroughPagingIterator.oneShot();
        }
        builder.addSubContext(new PageDownstreamContext(
            LOGGER,
            localNode,
            RECEIVER_PHASE_ID,
            "remoteCollectReceiver",
            consumer,
            pagingIterator,
            DataTypes.getStreamers(collectPhase.outputTypes()),
            ramAccountingContext,
            1
        ));
        return builder;
    }

    private void killRemoteContext() {
        transportKillJobsNodeAction.broadcast(new KillJobsRequest(Collections.singletonList(jobId)),
            new ActionListener<KillResponse>() {

                @Override
                public void onResponse(KillResponse killResponse) {
                    context.kill();
                }

                @Override
                public void onFailure(Throwable e) {
                    context.kill();
                }
            });
    }

    @Override
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
