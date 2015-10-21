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
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.bulk.SymbolBasedBulkShardProcessor;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SymbolBasedBulkShardProcessorContext implements ExecutionSubContext {

    private final static ESLogger LOGGER = Loggers.getLogger(SymbolBasedBulkShardProcessorContext.class);

    private ContextCallback callback = ContextCallback.NO_OP;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final SymbolBasedBulkShardProcessor bulkShardProcessor;
    private final SettableFuture<Void> closeFuture = SettableFuture.create();

    public SymbolBasedBulkShardProcessorContext(SymbolBasedBulkShardProcessor bulkShardProcessor) {
        this.bulkShardProcessor = bulkShardProcessor;
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        callback = MultiContextCallback.merge(callback, contextCallback);
    }

    public void start() {
        if(closed.get()){
            return;
        }
        bulkShardProcessor.close();
    }

    @Override
    public void close() {
        doClose(null);
    }
    
    private void doClose(@Nullable Throwable t) {
        if (!closed.getAndSet(true)) {
            callback.onClose(t, -1L);
            closeFuture.set(null);
        } else {
            try {
                closeFuture.get();
            } catch (Throwable e) {
                LOGGER.warn("Error while waiting for already running close {}", e);
            }
        }
    }

    public boolean add(String indexName,
                       String id,
                       @Nullable Symbol[] assignments,
                       @Nullable Object[] missingAssignments,
                       @Nullable String routing,
                       @Nullable Long version) {
        return bulkShardProcessor.add(indexName, id, assignments, missingAssignments, routing, version);
    }

    @Override
    public void kill() {
        bulkShardProcessor.kill();
        doClose(new CancellationException());
    }

    @Override
    public String name() {
        return "bulk-update-by-id";
    }

    @Override
    public SubContextMode subContextMode() {
        return SubContextMode.PASSIVE;
    }
}
