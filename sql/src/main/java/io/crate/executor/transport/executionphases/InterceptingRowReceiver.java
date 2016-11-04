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

package io.crate.executor.transport.executionphases;

import io.crate.data.Row;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.projectors.*;
import org.elasticsearch.action.ActionListener;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

class InterceptingRowReceiver implements RowReceiver, BiConsumer<Object, Throwable> {

    private final static Logger LOGGER = Loggers.getLogger(InterceptingRowReceiver.class);

    private final AtomicInteger upstreams = new AtomicInteger(2);
    private final UUID jobId;
    private final RowReceiver rowReceiver;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final CompletableFuture<Void> rowReceiverUpstreamFinished = new CompletableFuture<>();
    private final AtomicBoolean rowReceiverDone = new AtomicBoolean(false);
    private Throwable failure;

    InterceptingRowReceiver(UUID jobId,
                            RowReceiver rowReceiver,
                            InitializationTracker jobsInitialized,
                            TransportKillJobsNodeAction transportKillJobsNodeAction) {
        this.jobId = jobId;
        this.rowReceiver = rowReceiver;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        jobsInitialized.future.whenComplete(this);
    }

    @Override
    public void fail(@Nullable Throwable throwable) {
        if (rowReceiverDone.compareAndSet(false, true)) {
            if (throwable == null) {
                rowReceiverUpstreamFinished.complete(null);
            } else {
                rowReceiverUpstreamFinished.completeExceptionally(throwable);
            }
            tryForwardResult(throwable);
        }
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        fail(null);
    }

    @Override
    public void kill(Throwable throwable) {
        fail(throwable);
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return rowReceiverUpstreamFinished;
    }

    @Override
    public Result setNextRow(Row row) {
        return rowReceiver.setNextRow(row);
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
        rowReceiver.pauseProcessed(resumeable);
    }

    @Override
    public void accept(Object o, Throwable throwable) {
        tryForwardResult(throwable);
    }

    private void tryForwardResult(Throwable throwable) {
        if (throwable != null && (failure == null || failure instanceof InterruptedException)) {
            failure = Exceptions.unwrap(throwable);
        }
        if (upstreams.decrementAndGet() > 0) {
            return;
        }
        if (failure == null) {
            rowReceiver.finish(RepeatHandle.UNSUPPORTED);
        } else {
            transportKillJobsNodeAction.broadcast(
                new KillJobsRequest(Collections.singletonList(jobId)), new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                        LOGGER.trace("Killed {} jobs before forwarding the failure={}", killResponse.numKilled(), failure);
                        rowReceiver.fail(failure);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.trace("Failed to kill job, forwarding failure anyway...", e);
                        rowReceiver.fail(failure);
                    }
                });
        }
    }
}
