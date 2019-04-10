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

package io.crate.protocols.postgres;

import io.crate.Constants;
import io.crate.action.sql.ResultReceiver;
import io.crate.data.Row;
import io.crate.exceptions.SQLExceptions;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.ConnectTransportException;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public class RetryOnFailureResultReceiver implements ResultReceiver {

    private static final Logger LOGGER = LogManager.getLogger(RetryOnFailureResultReceiver.class);

    private final ClusterService clusterService;
    private final ClusterState initialState;
    private final ThreadContext threadContext;
    private final Predicate<String> hasIndex;
    private final ResultReceiver delegate;
    private final UUID jobId;
    private final BiConsumer<UUID, ResultReceiver> retryAction;
    private int attempt = 1;

    public RetryOnFailureResultReceiver(ClusterService clusterService,
                                        ClusterState initialState,
                                        ThreadContext threadContext,
                                        Predicate<String> hasIndex,
                                        ResultReceiver delegate,
                                        UUID jobId,
                                        BiConsumer<UUID, ResultReceiver> retryAction) {
        this.clusterService = clusterService;
        this.initialState = initialState;
        this.threadContext = threadContext;
        this.hasIndex = hasIndex;
        this.delegate = delegate;
        this.jobId = jobId;
        this.retryAction = retryAction;
    }

    @Override
    public void setNextRow(Row row) {
        delegate.setNextRow(row);
    }

    @Override
    public void batchFinished() {
        delegate.batchFinished();
    }

    @Override
    public void allFinished(boolean interrupted) {
        delegate.allFinished(interrupted);
    }

    @Override
    public void fail(@Nonnull Throwable wrappedError) {
        final Throwable error = SQLExceptions.unwrap(wrappedError);
        if (attempt <= Constants.MAX_SHARD_MISSING_RETRIES &&
            (SQLExceptions.isShardFailure(error) || error instanceof ConnectTransportException || indexWasTemporaryUnavailable(error))) {

            if (clusterService.state().blocks().hasGlobalBlockWithStatus(RestStatus.SERVICE_UNAVAILABLE)) {
                delegate.fail(error);
            } else {
                ClusterStateObserver clusterStateObserver =
                    new ClusterStateObserver(initialState, clusterService, null, LOGGER, threadContext);
                clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        attempt += 1;
                        retry();
                    }

                    @Override
                    public void onClusterServiceClose() {
                        delegate.fail(error);
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        delegate.fail(error);
                    }
                });
            }
        } else {
            delegate.fail(error);
        }
    }

    private boolean indexWasTemporaryUnavailable(Throwable t) {
        return t instanceof IndexNotFoundException && hasIndex.test(((IndexNotFoundException) t).getIndex().getName());
    }

    private void retry() {
        UUID newJobId = UUID.randomUUID();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Retrying statement due to a shard failure, attempt={}, jobId={}->{}", attempt, jobId, newJobId);
        }
        retryAction.accept(newJobId, this);
    }

    @Override
    public CompletableFuture<?> completionFuture() {
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
