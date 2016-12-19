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
import io.crate.executor.transport.ShardResponse;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.metadata.PartitionName;
import io.crate.planner.node.dml.UpsertById;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UpsertByIdContext extends AbstractExecutionSubContext {

    private final static Logger LOGGER = Loggers.getLogger(UpsertByIdContext.class);

    private final ShardUpsertRequest request;
    private final UpsertById.Item item;
    private final SettableFuture<Long> resultFuture = SettableFuture.create();
    private final BulkRequestExecutor transportShardUpsertActionDelegate;

    public UpsertByIdContext(int id,
                             ShardUpsertRequest request,
                             UpsertById.Item item,
                             BulkRequestExecutor transportShardUpsertActionDelegate) {
        super(id, LOGGER);
        this.request = request;
        this.item = item;
        this.transportShardUpsertActionDelegate = transportShardUpsertActionDelegate;
        assert !request.continueOnError() : "continueOnError flag is expected to be set to false for upsertById";
    }

    public SettableFuture<Long> resultFuture() {
        return resultFuture;
    }

    @Override
    protected void innerStart() {
        transportShardUpsertActionDelegate.execute(request, new ActionListener<ShardResponse>() {
            @Override
            public void onResponse(ShardResponse updateResponse) {
                if (future.closed()) {
                    return;
                }
                if (updateResponse.failure() != null) {
                    onFailure(updateResponse.failure());
                    return;
                }
                resultFuture.set(1L);
                close(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (future.closed()) {
                    return;
                }
                e = ExceptionsHelper.unwrapCause(e);
                if (item.insertValues() == null
                    && (e instanceof DocumentMissingException
                        || e instanceof VersionConflictEngineException)) {
                    // on updates, set affected row to 0 if document is not found or version conflicted
                    resultFuture.set(0L);
                    close(null);
                } else if (PartitionName.isPartition(request.index())
                           && e instanceof IndexNotFoundException) {
                    // index missing exception on a partition should never bubble, set affected row to 0
                    resultFuture.set(0L);
                    close(null);
                } else {
                    resultFuture.setException(e);
                    close(e);
                }
            }
        });
    }


    @Override
    public void innerKill(@Nonnull Throwable throwable) {
        resultFuture.cancel(false);
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        if (!resultFuture.isDone()) {
            if (t == null) {
                resultFuture.cancel(false);
            } else {
                resultFuture.setException(t);
            }
        }
    }

    @Override
    public String name() {
        return "upsert-by-id";
    }
}
