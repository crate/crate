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
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.planner.node.dml.UpsertByIdNode;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UpsertByIdContext extends AbstractExecutionSubContext {

    private static final ESLogger logger = Loggers.getLogger(ExecutionSubContext.class);

    private final ShardUpsertRequest request;
    private final UpsertByIdNode.Item item;
    private final SettableFuture<TaskResult> futureResult;
    private final BulkRequestExecutor<ShardUpsertRequest> transportShardUpsertActionDelegate;

    public UpsertByIdContext(int id,
                             ShardUpsertRequest request,
                             UpsertByIdNode.Item item,
                             SettableFuture<TaskResult> futureResult,
                             BulkRequestExecutor<ShardUpsertRequest> transportShardUpsertActionDelegate) {
        super(id);
        this.request = request;
        this.item = item;
        this.futureResult = futureResult;
        this.transportShardUpsertActionDelegate = transportShardUpsertActionDelegate;
    }

    @Override
    protected void innerStart() {
        transportShardUpsertActionDelegate.execute(request, new ActionListener<ShardUpsertResponse>() {
            @Override
            public void onResponse(ShardUpsertResponse updateResponse) {
                if (future.closed()) {
                    return;
                }
                int location = updateResponse.itemIndices().get(0);

                ShardUpsertResponse.Failure failure = updateResponse.failures().get(location);
                if (failure == null) {
                    futureResult.set(TaskResult.ONE_ROW);
                } else {
                    if (logger.isDebugEnabled()) {
                        if (failure.versionConflict()) {
                            logger.debug("Upsert of document with id {} failed because of a version conflict", failure.id());
                        } else {
                            logger.debug("Upsert of document with id {} failed {}", failure.id(), failure.message());
                        }
                    }
                    futureResult.set(TaskResult.ZERO);
                }
                close(null);
            }

            @Override
            public void onFailure(Throwable e) {
                if (future.closed()) {
                    return;
                }
                e = ExceptionsHelper.unwrapCause(e);
                if (item.insertValues() == null
                        && (e instanceof DocumentMissingException
                        || e instanceof VersionConflictEngineException)) {
                    // on updates, set affected row to 0 if document is not found or version conflicted
                    futureResult.set(TaskResult.ZERO);
                } else {
                    futureResult.setException(e);
                }
                close(e);
            }
        });
    }


    @Override
    public void innerKill(@Nonnull Throwable throwable) {
        futureResult.cancel(true);
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        if (!futureResult.isDone()) {
            if (t == null) {
                futureResult.set(TaskResult.EMPTY_RESULT);
            } else {
                futureResult.setException(t);
            }
        }
    }

    @Override
    public String name() {
        return "upsert-by-id";
    }
}
