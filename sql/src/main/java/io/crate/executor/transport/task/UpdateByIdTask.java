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

package io.crate.executor.transport.task;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.exceptions.Exceptions;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.ShardUpdateRequest;
import io.crate.executor.transport.ShardUpdateResponse;
import io.crate.executor.transport.TransportShardUpdateAction;
import io.crate.planner.node.dml.UpdateByIdNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.UUID;

public class UpdateByIdTask extends AsyncChainedTask {

    private final TransportShardUpdateAction transport;
    private final ActionListener<ShardUpdateResponse> listener;
    private final ShardUpdateRequest request;

    private static class UpdateResponseListener implements ActionListener<ShardUpdateResponse> {

        private final SettableFuture<TaskResult> future;

        private UpdateResponseListener(SettableFuture<TaskResult> future) {
            this.future = future;
        }

        @Override
        public void onResponse(ShardUpdateResponse updateResponse) {
            future.set(TaskResult.ONE_ROW);
        }

        @Override
        public void onFailure(Throwable e) {
            e = Exceptions.unwrap(e);
            if (e instanceof VersionConflictEngineException
                    || e instanceof DocumentMissingException) {
                future.set(TaskResult.ZERO);
            } else {
                future.setException(e);
            }
        }
    }

    public UpdateByIdTask(UUID jobId, TransportShardUpdateAction transport, UpdateByIdNode node) {
        super(jobId);
        this.transport = transport;

        this.request = buildUpdateRequest(node);
        listener = new UpdateResponseListener(result);
    }

    @Override
    public void start() {
        transport.execute(this.request, this.listener);
    }

    protected ShardUpdateRequest buildUpdateRequest(UpdateByIdNode node) {
        ShardUpdateRequest request = new ShardUpdateRequest(node.index(), node.id());
        request.routing(node.routing());
        if (node.version().isPresent()) {
            request.version(node.version().get());
        }
        request.assignments(node.assignments());
        request.missingAssignments(node.missingAssignments());
        request.missingAssignmentsColumns(node.missingAssignmentsColumns());
        return request;
    }
}
