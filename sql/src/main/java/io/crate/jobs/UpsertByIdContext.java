/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.jobs;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.executor.transport.SymbolBasedShardUpsertRequest;
import io.crate.planner.node.dml.SymbolBasedUpsertByIdNode;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.SymbolBasedTransportShardUpsertActionDelegate;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class UpsertByIdContext implements ExecutionSubContext {

    private final SymbolBasedShardUpsertRequest request;
    private final SymbolBasedUpsertByIdNode.Item item;
    private final SettableFuture futureResult;
    private final SymbolBasedTransportShardUpsertActionDelegate transportShardUpsertActionDelegate;

    private final ArrayList<ContextCallback> callbacks = new ArrayList<>(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static final ESLogger logger = Loggers.getLogger(ExecutionSubContext.class);

    public UpsertByIdContext(SymbolBasedShardUpsertRequest request,
                             SymbolBasedUpsertByIdNode.Item item,
                             SettableFuture futureResult,
                             SymbolBasedTransportShardUpsertActionDelegate transportShardUpsertActionDelegate){
        this.request = request;
        this.item = item;
        this.futureResult = futureResult;
        this.transportShardUpsertActionDelegate = transportShardUpsertActionDelegate;
    }

    public void start() {
        transportShardUpsertActionDelegate.execute(request, new ActionListener<ShardUpsertResponse>() {
            @Override
            public void onResponse(ShardUpsertResponse updateResponse) {
                int location = updateResponse.itemIndices().get(0);
                if (updateResponse.responses().get(location) != null) {
                    futureResult.set(TaskResult.ONE_ROW);
                } else {
                    ShardUpsertResponse.Failure failure = updateResponse.failures().get(location);
                    if (logger.isDebugEnabled()) {
                        if (failure.versionConflict()) {
                            logger.debug("Upsert of document with id {} failed because of a version conflict", failure.id());
                        } else {
                            logger.debug("Upsert of document with id {} failed {}", failure.id(), failure.message());
                        }
                    }
                    futureResult.set(TaskResult.ZERO);
                }
                close();
            }

            @Override
            public void onFailure(Throwable e) {
                e = ExceptionsHelper.unwrapCause(e);
                if (item.insertValues() == null
                        && (e instanceof DocumentMissingException
                        || e instanceof VersionConflictEngineException)) {
                    // on updates, set affected row to 0 if document is not found or version conflicted
                    futureResult.set(TaskResult.ZERO);
                } else {
                    futureResult.setException(e);
                }
                close();
            }
        });
    }


    @Override
    public void addCallback(ContextCallback contextCallback) {
        callbacks.add(contextCallback);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            for (ContextCallback callback : callbacks) {
                callback.onClose();
            }
        }
    }

    @Override
    public void kill() {
        throw new UnsupportedOperationException("kill is not implemented");
    }
}
