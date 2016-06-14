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

package io.crate.operation;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.FetchProperties;
import io.crate.concurrent.CompletionListenable;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionMultiListener;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.StreamBucket;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.Requirements;
import io.crate.operation.projectors.RowReceiver;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class ClientPagingReceiver implements RowReceiver, CompletionListenable {

    final SettableFuture<TaskResult> resultFuture;
    private final StreamBucket.Builder bucketBuilder;
    private final Collection<? extends DataType> outputTypes;

    private CompletionListener listener = CompletionListener.NO_OP;
    private RowUpstream rowUpstream;
    private FetchCallback callback;
    private Throwable killed;
    private FetchProperties fetchProperties;

    public ClientPagingReceiver(FetchProperties fetchProperties,
                                SettableFuture<TaskResult> resultFuture,
                                Collection<? extends DataType> outputTypes) {
        this.fetchProperties = fetchProperties;
        this.resultFuture = resultFuture;
        this.bucketBuilder = new StreamBucket.Builder(DataTypes.getStreamer(outputTypes));
        this.outputTypes = outputTypes;
    }

    @Override
    public boolean setNextRow(Row row) {
        if (killed != null) {
            return false;
        }

        try {
            bucketBuilder.add(row);
        } catch (IOException e) {
            fail(e);
            return false;
        }
        if (bucketBuilder.size() >= fetchProperties.fetchSize()) {
            if (fetchProperties.closeContext()) {
                emitResult(true);
                return false;
            } else {
                rowUpstream.pause();
                emitResult(false);
            }
        }
        return true;
    }

    private void emitResult(boolean isLast) {
        if (isLast) {
            // listener is called before emitting to avoid flakyness in tests that assert that the context is gone
            // after they receive the result
            listener.onSuccess(null);
        }
        if (!resultFuture.isDone()) {
            try {
                resultFuture.set(new QueryResult(bucketBuilder.build()));
            } catch (IOException e) {
                resultFuture.setException(e);
            }
        } else {
            FetchCallback fetchCallback = callback;
            callback = null;
            try {
                fetchCallback.onResult(bucketBuilder.build(), isLast);
            } catch (IOException e) {
                fetchCallback.onError(e);
            }
        }
        if (!isLast) {
            bucketBuilder.reset();
        }
    }

    public void fetch(FetchProperties fetchProperties, FetchCallback callback) {
        if (this.callback != null) {
            callback.onError(new IllegalStateException("There may only be one active fetch operation at a time per cursorId"));
            return;
        }
        this.callback = callback;
        this.fetchProperties = fetchProperties;
        rowUpstream.resume(false);
    }

    @Override
    public void finish() {
        emitResult(true);
    }

    @Override
    public void fail(Throwable throwable) {
        if (!resultFuture.isDone()) {
            resultFuture.setException(throwable);
        } else {
            callback.onError(throwable);
            callback = null;
        }
        listener.onFailure(throwable);
    }

    @Override
    public void kill(Throwable throwable) {
        killed = throwable;
        listener.onFailure(throwable);
    }

    @Override
    public void prepare() {
    }

    @Override
    public void setUpstream(RowUpstream rowUpstream) {
        this.rowUpstream = rowUpstream;
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Override
    public void addListener(CompletionListener listener) {
        this.listener = CompletionMultiListener.merge(this.listener, listener);
    }

    public Collection<? extends DataType> outputTypes() {
        return outputTypes;
    }

    public interface FetchCallback {
        void onResult(Bucket rows, boolean isLast);
        void onError(Throwable t);
    }
}
