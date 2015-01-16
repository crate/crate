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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.exceptions.Exceptions;
import io.crate.executor.TaskResult;
import io.crate.planner.node.dml.ESUpdateNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;

public class ESUpdateByIdTask extends AbstractESUpdateTask {

    private final TransportUpdateAction transport;
    private final ActionListener<UpdateResponse> listener;
    private final UpdateRequest request;

    private static class UpdateResponseListener implements ActionListener<UpdateResponse> {

        private final SettableFuture<TaskResult> future;

        private UpdateResponseListener(SettableFuture<TaskResult> future) {
            this.future = future;
        }

        @Override
        public void onResponse(UpdateResponse updateResponse) {
            if (updateResponse.getGetResult().isExists()) {
                future.set(TaskResult.ONE_ROW);
            } else {
                future.set(TaskResult.ZERO);
            }
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

    public ESUpdateByIdTask(TransportUpdateAction transport, ESUpdateNode node) {
        super(node);
        this.transport = transport;

        assert node.ids().size() == 1;
        this.request = buildUpdateRequest(node);
        listener = new UpdateResponseListener(result);
    }

    @Override
    public void start() {
        transport.execute(this.request, this.listener);
    }

    protected UpdateRequest buildUpdateRequest(ESUpdateNode node) {
        UpdateRequest request = new UpdateRequest(node.indices()[0],
                Constants.DEFAULT_MAPPING_TYPE, node.ids().get(0));
        request.fields(node.columns());
        request.paths(node.updateDoc());
        if (node.version().isPresent()) {
            request.version(node.version().get());
        } else {
            request.retryOnConflict(Constants.UPDATE_RETRY_ON_CONFLICT);
        }
        request.routing(node.routingValues().get(0));
        return request;
    }
}
