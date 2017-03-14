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

import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class InterceptingBatchConsumer implements BatchConsumer {

    private final static ESLogger LOGGER = Loggers.getLogger(InterceptingBatchConsumer.class);

    private final AtomicInteger consumerInvokedAndJobInitialized = new AtomicInteger(2);
    private final UUID jobId;
    private final BatchConsumer consumer;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final AtomicBoolean consumerAccepted = new AtomicBoolean(false);

    private Throwable failure = null;
    private BatchIterator iterator = null;

    InterceptingBatchConsumer(UUID jobId,
                              BatchConsumer consumer,
                              InitializationTracker jobsInitialized,
                              TransportKillJobsNodeAction transportKillJobsNodeAction) {
        this.jobId = jobId;
        this.consumer = consumer;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        jobsInitialized.future.whenComplete((o, f) -> tryForwardResult(f));
    }

    @Override
    public void accept(BatchIterator iterator, @Nullable Throwable failure) {
        if (consumerAccepted.compareAndSet(false, true)) {
            this.iterator = iterator;
            tryForwardResult(failure);
        }
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
            consumer.accept(iterator, null);
        } else {
            transportKillJobsNodeAction.broadcast(
                new KillJobsRequest(Collections.singletonList(jobId)), new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                        LOGGER.trace("Killed {} jobs before forwarding the failure={}", killResponse.numKilled(), failure);
                        consumer.accept(null, failure);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.trace("Failed to kill job, forwarding failure anyway...", e);
                        consumer.accept(null, failure);
                    }
                });
        }
    }

    @Override
    public String toString() {
        return "InterceptingRowReceiver{" +
               "consumerInvokedAndJobInitilaized=" + consumerInvokedAndJobInitialized +
               ", jobId=" + jobId +
               ", consumer=" + consumer +
               ", rowReceiverDone=" + consumerAccepted +
               ", failure=" + failure +
               '}';
    }
}
