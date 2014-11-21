/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.AsyncChainedTask;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.deletebyquery.CrateTransportDeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;

import java.io.IOException;

public class ESDeleteByQueryTask extends AsyncChainedTask {

    private final ESDeleteByQueryNode deleteByQueryNode;
    private final CrateTransportDeleteByQueryAction transportDeleteByQueryAction;
    private final ESQueryBuilder queryBuilder;

    public ESDeleteByQueryTask(ESDeleteByQueryNode deleteByQueryNode,
                               CrateTransportDeleteByQueryAction transportDeleteByQueryAction) {
        this.deleteByQueryNode = deleteByQueryNode;
        this.transportDeleteByQueryAction = transportDeleteByQueryAction;
        this.queryBuilder = new ESQueryBuilder();
    }

    @Override
    public void start() {
        final DeleteByQueryRequest request = new DeleteByQueryRequest();

        try {
            request.source(queryBuilder.convert(deleteByQueryNode), false);
            request.indices(deleteByQueryNode.indices());
            request.routing(deleteByQueryNode.whereClause().clusteredBy().orNull());

            transportDeleteByQueryAction.execute(request, new ActionListener<DeleteByQueryResponse>() {
                @Override
                public void onResponse(DeleteByQueryResponse deleteByQueryResponses) {
                    result.set(TaskResult.ROW_COUNT_UNKNOWN);
                }

                @Override
                public void onFailure(Throwable e) {
                    result.setException(e);
                }
            });
        } catch (IOException e) {
            result.setException(e);
        }
    }
}
