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

package io.crate.execution.engine;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.execution.support.ThreadPools;
import io.crate.user.Role;

class InterceptingRowConsumer implements RowConsumer {

    private static final Logger LOGGER = LogManager.getLogger(InterceptingRowConsumer.class);

    private final AtomicInteger consumerInvokedAndJobInitialized = new AtomicInteger(2);
    private final UUID jobId;
    private final RowConsumer consumer;
    private final Executor executor;
    private final ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction;
    private final AtomicBoolean consumerAccepted = new AtomicBoolean(false);

    private Throwable failure = null;
    private BatchIterator<Row> iterator = null;

    InterceptingRowConsumer(UUID jobId,
                            RowConsumer consumer,
                            InitializationTracker jobsInitialized,
                            Executor executor,
                            ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction) {
        this.jobId = jobId;
        this.consumer = consumer;
        this.executor = executor;
        this.killNodeAction = killNodeAction;
        jobsInitialized.future.whenComplete((o, f) -> tryForwardResult(f));
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (consumerAccepted.compareAndSet(false, true)) {
            this.iterator = iterator;
            tryForwardResult(failure);
        }
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return consumer.completionFuture();
    }

    private void tryForwardResult(Throwable throwable) {
        if (throwable != null && (failure == null || failure instanceof InterruptedException)) {
            failure = SQLExceptions.unwrap(throwable);
        }
        if (consumerInvokedAndJobInitialized.decrementAndGet() > 0) {
            return;
        }
        if (failure == null) {
            assert iterator != null : "iterator must be present";
            ThreadPools.forceExecute(executor, () -> consumer.accept(iterator, null));
        } else {
            consumer.accept(null, failure);
            KillJobsNodeRequest killRequest = new KillJobsNodeRequest(
                List.of(),
                List.of(jobId),
                Role.CRATE_USER.name(),
                "An error was encountered: " + failure
            );
            killNodeAction
                .execute(killRequest)
                .whenComplete(
                    (resp, t) -> {
                        if (LOGGER.isTraceEnabled()) {
                            if (t == null) {
                                LOGGER.trace("Killed {} contexts for jobId={} forwarding the failure={}",
                                             resp.numKilled(),
                                             jobId,
                                             failure);
                            } else {
                                LOGGER.trace("Failed to kill jobId={}, forwarding failure={} anyway",
                                             jobId,
                                             failure);
                            }
                        }
                    }
                );
        }
    }

    @Override
    public String toString() {
        return "InterceptingBatchConsumer{" +
               "consumerInvokedAndJobInitialized=" + consumerInvokedAndJobInitialized +
               ", jobId=" + jobId +
               ", consumer=" + consumer +
               ", rowReceiverDone=" + consumerAccepted +
               ", failure=" + failure +
               '}';
    }
}
