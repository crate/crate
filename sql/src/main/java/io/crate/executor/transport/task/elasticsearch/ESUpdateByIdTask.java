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
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.Constants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

public class ESUpdateByIdTask extends AbstractESUpdateTask {
    public static final int UPDATE_RETRY_ON_CONFLICT = 3;

    private final TransportUpdateAction transport;
    private final ActionListener<UpdateResponse> listener;
    private final UpdateRequest request;

    private static class UpdateResponseListener implements ActionListener<UpdateResponse> {

        private final SettableFuture<Object[][]> future;

        private UpdateResponseListener(SettableFuture<Object[][]> future) {
            this.future = future;
        }

        @Override
        public void onResponse(UpdateResponse updateResponse) {
            future.set(new Object[][]{new Object[]{updateResponse.getGetResult().isExists() ? 1L : 0L}});
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }

    public ESUpdateByIdTask(TransportUpdateAction transport, ESUpdateNode node) {
        super(node);
        this.transport = transport;

        assert node.primaryKeyValues().length == 1;
        this.request = buildUpdateRequest(node);
        listener = new UpdateResponseListener(result);
    }

    @Override
    public void start() {
        transport.execute(this.request, this.listener);
    }

    protected UpdateRequest buildUpdateRequest(ESUpdateNode node) {
        UpdateRequest request = new UpdateRequest(node.index(),
                Constants.DEFAULT_MAPPING_TYPE, node.primaryKeyValues()[0]);
        request.fields(node.columns());
        request.paths(node.updateDoc());
        request.retryOnConflict(UPDATE_RETRY_ON_CONFLICT);
        return request;
    }
}
