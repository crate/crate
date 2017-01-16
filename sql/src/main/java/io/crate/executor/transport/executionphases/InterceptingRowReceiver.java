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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.data.BatchConsumer;
import io.crate.operation.data.BatchCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class InterceptingRowReceiver implements BatchConsumer, FutureCallback<Void> {

    private final static ESLogger LOGGER = Loggers.getLogger(InterceptingRowReceiver.class);

    private final AtomicInteger upstreams = new AtomicInteger(2);
    private final UUID jobId;
    private final BatchConsumer rowReceiver;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final AtomicBoolean rowReceiverDone = new AtomicBoolean(false);
    private Throwable failure;
    private BatchCursor cursor;

    InterceptingRowReceiver(UUID jobId,
                            BatchConsumer rowReceiver,
                            InitializationTracker jobsInitialized,
                            TransportKillJobsNodeAction transportKillJobsNodeAction) {
        this.jobId = jobId;
        this.rowReceiver = rowReceiver;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        Futures.addCallback(jobsInitialized.future, this);
    }

    @Override
    public void accept(BatchCursor batchCursor) {
        System.err.println("IRR: accept");
        this.cursor = batchCursor;
        tryForwardResult(null);
    }

    @Override
    public void fail(Throwable throwable) {
        System.err.println("IRR: fail");
        tryForwardResult(throwable);
    }

    @Override
    public void onSuccess(@Nullable Void result) {
        System.err.println("IRR: onSuccess");
        tryForwardResult(null);
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
        System.err.println("IRR: onFailure");
        tryForwardResult(t);
    }

    private void tryForwardResult(Throwable throwable) {
        if (throwable != null && (failure == null || failure instanceof InterruptedException)) {
            failure = Exceptions.unwrap(throwable);
        }
        if (upstreams.decrementAndGet() > 0) {
            return;
        }

        if (failure == null) {
            assert cursor != null: "cursor should be set if no failure";
            rowReceiver.accept(cursor);
        } else {
            transportKillJobsNodeAction.broadcast(
                new KillJobsRequest(Collections.singletonList(jobId)), new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                        LOGGER.trace("Killed {} jobs before forwarding the failure={}", killResponse.numKilled(), failure);
                        rowReceiver.fail(failure);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.trace("Failed to kill job, forwarding failure anyway...", e);
                        rowReceiver.fail(failure);
                    }
                });
        }
    }

}
