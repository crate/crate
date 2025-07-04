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

package io.crate.session;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.ConnectTransportException;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Row;
import io.crate.exceptions.SQLExceptions;

public class RetryOnFailureResultReceiver<T> implements ResultReceiver<T> {

    private static final Logger LOGGER = LogManager.getLogger(RetryOnFailureResultReceiver.class);

    private final ClusterService clusterService;
    private final ResultReceiver<T> delegate;
    private final UUID jobId;
    private final BiConsumer<UUID, ResultReceiver<T>> retryAction;
    private final int maxRetryCount;
    private int attempt = 1;

    public RetryOnFailureResultReceiver(int maxRetryCount,
                                        ClusterService clusterService,
                                        ResultReceiver<T> delegate,
                                        UUID jobId,
                                        BiConsumer<UUID, ResultReceiver<T>> retryAction) {
        this.maxRetryCount = maxRetryCount;
        this.clusterService = clusterService;
        this.delegate = delegate;
        this.jobId = jobId;
        this.retryAction = retryAction;
    }

    @Override
    @Nullable
    public CompletableFuture<Void> setNextRow(Row row) throws Exception {
        return delegate.setNextRow(row);
    }

    @Override
    public void batchFinished() {
        delegate.batchFinished();
    }

    @Override
    public void allFinished() {
        delegate.allFinished();
    }

    @Override
    public void fail(Throwable wrappedError) {
        final Throwable error = SQLExceptions.unwrap(wrappedError);
        if (attempt <= maxRetryCount &&
            (SQLExceptions.isShardNotAvailable(error) || error instanceof ConnectTransportException)) {

            if (clusterService.state().blocks().hasGlobalBlockWithStatus(RestStatus.SERVICE_UNAVAILABLE)) {
                delegate.fail(error);
            } else {
                attempt += 1;
                UUID newJobId = UUIDs.dirtyUUID();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Retrying statement due to a shard failure, attempt={}, jobId={}->{}", attempt, jobId, newJobId);
                }
                retryAction.accept(newJobId, this);
            }
        } else {
            delegate.fail(error);
        }
    }

    @Override
    public CompletableFuture<T> completionFuture() {
        return delegate.completionFuture();
    }

    @Override
    public String toString() {
        return "RetryOnFailureResultReceiver{" +
               "delegate=" + delegate +
               ", jobId=" + jobId +
               ", attempt=" + attempt +
               '}';
    }
}
