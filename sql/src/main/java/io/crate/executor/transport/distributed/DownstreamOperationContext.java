/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.distributed;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.UnknownUpstreamFailure;
import io.crate.operation.DownstreamOperation;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicInteger;

public class DownstreamOperationContext {

    private static final ESLogger logger = Loggers.getLogger(DownstreamOperationContext.class);

    private final AtomicInteger mergeOperationsLeft;
    private final DownstreamOperation downstreamOperation;
    private final SettableFuture<Bucket> listener;
    private final Streamer<?>[] streamers;
    private final DistributedRequestContextManager.DoneCallback doneCallback;
    private boolean needsMoreRows = true;
    private final Object lock = new Object();

    public DownstreamOperationContext(DownstreamOperation downstreamOperation,
                                      final SettableFuture<Bucket> listener,
                                      Streamer<?>[] streamers,
                                      DistributedRequestContextManager.DoneCallback doneCallback) {
        this.mergeOperationsLeft = new AtomicInteger(downstreamOperation.numUpstreams());
        this.downstreamOperation = downstreamOperation;
        this.listener = listener;
        Futures.addCallback(downstreamOperation.result(), new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket result) {
                listener.set(result);
            }

            @Override
            public void onFailure(Throwable t) {
                listener.setException(t);

            }
        });
        this.streamers = streamers;
        this.doneCallback = doneCallback;
    }

    public void addFailure(@Nullable Throwable failure) {
        if (failure != null) {
            logger.error("addFailure local", failure);
        } else {
            failure = new UnknownUpstreamFailure();
        }
        try {
            boolean firstFailure = listener.setException(failure);
            logger.trace("addFailure first: {}", firstFailure);
        } finally {
            if (mergeOperationsLeft.decrementAndGet() == 0) {
                doneCallback.finished();
                downstreamOperation.finished();
            }
        }
    }

    public void add(Bucket rows) {
        assert rows != null;
        logger.trace("add rows.size: {}", rows.size());
        synchronized (lock) {
            if (needsMoreRows) {
                try {
                    needsMoreRows = downstreamOperation.addRows(rows);
                } catch (Exception e) {
                    logger.error("failed to add rows to downstreamOperation", e);
                    listener.setException(e);
                }
            }
        }

        if (mergeOperationsLeft.decrementAndGet() == 0) {
            doneCallback.finished();
            downstreamOperation.finished();
        }
    }

    public Streamer<?>[] streamers() {
        return streamers;
    }
}
