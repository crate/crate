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
import io.crate.action.sql.ResultReceiver;
import io.crate.core.collections.Row;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.Requirements;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.action.ActionListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class InterceptingRowReceiver implements RowReceiver, FutureCallback<Void> {

    private final AtomicInteger upstreams = new AtomicInteger(2);
    private final UUID jobId;
    private final ResultReceiver resultReceiver;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final AtomicBoolean rowReceiverDone = new AtomicBoolean(false);
    private Throwable failure;

    InterceptingRowReceiver(UUID jobId,
                            ResultReceiver resultReceiver,
                            InitializationTracker jobsInitialized,
                            TransportKillJobsNodeAction transportKillJobsNodeAction) {
        this.jobId = jobId;
        this.resultReceiver = resultReceiver;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        Futures.addCallback(jobsInitialized.future, this);
    }

    @Override
    public void fail(Throwable throwable) {
        if (rowReceiverDone.compareAndSet(false, true)) {
            tryForwardResult(throwable);
        }
    }

    @Override
    public void kill(Throwable throwable) {
        fail(throwable);
    }

    @Override
    public void prepare() {
    }

    @Override
    public void setUpstream(RowUpstream rowUpstream) {
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Override
    public boolean setNextRow(Row row) {
        return resultReceiver.setNextRow(row);
    }

    @Override
    public void finish() {
        fail(null);
    }

    @Override
    public void onSuccess(@Nullable Void result) {
        tryForwardResult(null);
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
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
            resultReceiver.finish();
        } else {
            transportKillJobsNodeAction.executeKillOnAllNodes(
                new KillJobsRequest(Collections.singletonList(jobId)), new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                        resultReceiver.fail(failure);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        resultReceiver.fail(failure);
                    }
                });
        }
    }
}
