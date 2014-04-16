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
import io.crate.executor.transport.task.AbstractChainedTask;
import io.crate.planner.node.ddl.ESCreateAliasNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;

import java.util.List;

public class ESCreateAliasTask extends AbstractChainedTask<Object[][]> {

    private static class CreateAliasListener implements ActionListener<IndicesAliasesResponse> {

        private final SettableFuture<Object[][]> future;

        private CreateAliasListener(SettableFuture<Object[][]> future) {
            this.future = future;
        }

        @Override
        public void onResponse(IndicesAliasesResponse indicesAliasesResponse) {
            if (indicesAliasesResponse.isAcknowledged()) {
                future.set(new Object[][]{new Object[]{1L}});
            } else {
                future.set(new Object[][]{new Object[]{0L}});
            }
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }

    private final TransportIndicesAliasesAction transport;
    private final IndicesAliasesRequest request;
    private final CreateAliasListener listener;

    public ESCreateAliasTask(ESCreateAliasNode node, TransportIndicesAliasesAction transport) {
        super();
        this.transport = transport;
        listener = new CreateAliasListener(this.result);

        request = new IndicesAliasesRequest().addAlias(
                node.aliasName(),
                node.tableName()
        );
    }

    @Override
    protected void doStart(List<Object[][]> upstreamResults) {
        transport.execute(request, listener);
    }
}
