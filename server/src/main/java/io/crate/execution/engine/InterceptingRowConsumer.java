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
import io.crate.role.Role;

class InterceptingRowConsumer implements RowConsumer {

    private static final Logger LOGGER = LogManager.getLogger(InterceptingRowConsumer.class);

    private final UUID jobId;
    private final RowConsumer consumer;
    private final ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction;

    private final InitializationTracker initTracker;

    InterceptingRowConsumer(UUID jobId,
                            RowConsumer consumer,
                            InitializationTracker initTracker,
                            ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction) {
        this.jobId = jobId;
        this.consumer = consumer;
        this.initTracker = initTracker;
        this.killNodeAction = killNodeAction;
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        consumer.accept(iterator, failure);
        initTracker.future.whenComplete((_, err) -> {
            Throwable t = SQLExceptions.unwrap(failure == null ? err : failure);
            if (t == null) {
                return;
            }
            if (iterator != null) {
                iterator.kill(t);
            }
            KillJobsNodeRequest killRequest = new KillJobsNodeRequest(
                List.of(),
                List.of(jobId),
                Role.CRATE_USER.name(),
                "An error was encountered: " + t
            );
            killNodeAction.execute(killRequest).whenComplete((resp, killErr) -> {
                if (killErr == null) {
                    LOGGER.trace(
                        "Killed {} contexts for jobId={} due to failure={}",
                        resp.numKilled(),
                        jobId,
                        t);
                } else {
                    LOGGER.trace(
                        "Failed to kill jobId={} due to failure={}",
                        jobId,
                        t);
                }
            });
        });
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return consumer.completionFuture();
    }

    @Override
    public void kill(Throwable throwable) {
        consumer.kill(throwable);
    }

    @Override
    public String toString() {
        return "InterceptingRowConsumer{" +
               "initTracker=" + initTracker +
               ", jobId=" + jobId +
               ", consumer=" + consumer +
               '}';
    }
}
