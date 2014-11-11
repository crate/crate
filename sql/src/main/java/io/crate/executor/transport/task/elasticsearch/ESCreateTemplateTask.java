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
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.AbstractChainedTask;
import io.crate.planner.node.ddl.ESCreateTemplateNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;

import java.util.List;

public class ESCreateTemplateTask extends AbstractChainedTask<RowCountResult> {

    private static class CreateTemplateListener implements ActionListener<PutIndexTemplateResponse> {

        private final SettableFuture<RowCountResult> future;

        private CreateTemplateListener(SettableFuture<RowCountResult> future) {
            this.future = future;
        }

        @Override
        public void onResponse(PutIndexTemplateResponse putIndexTemplateResponse) {
            if (putIndexTemplateResponse.isAcknowledged()) {
                future.set(TaskResult.ONE_ROW);
            } else {
                future.set(TaskResult.ZERO);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }

    private final TransportPutIndexTemplateAction transport;
    private final PutIndexTemplateRequest request;
    private final CreateTemplateListener listener;

    public ESCreateTemplateTask(ESCreateTemplateNode node, TransportPutIndexTemplateAction transport) {
        super();
        this.transport = transport;
        this.listener = new CreateTemplateListener(result);
        this.request = buildRequest(node);
    }

    @Override
    protected void doStart(List<TaskResult> upstreamResults) {
        transport.execute(request, listener);
    }

    private PutIndexTemplateRequest buildRequest(ESCreateTemplateNode node) {
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(node.templateName())
                .mapping(Constants.DEFAULT_MAPPING_TYPE, node.mapping())
                .create(true)
                .settings(node.indexSettings())
                .template(node.indexMatch());
        if (node.alias() != null) {
            templateRequest.alias(new Alias(node.alias()));
        }
        return templateRequest;
    }
}
