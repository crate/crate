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


import io.crate.Constants;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.planner.node.ddl.MappingUpdateNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class UpdateMappingTask extends AbstractChainedTask<RowCountResult> {

    private final TransportPutMappingAction putMappingAction;
    private final MappingUpdateNode node;

    public UpdateMappingTask(TransportPutMappingAction putMappingAction,
                             MappingUpdateNode node){
        super();
        this.putMappingAction = putMappingAction;
        this.node = node;
    }

    @Override
    protected void doStart(List<TaskResult> upstreamResults) {
        IndexMetaData indexMetaData = node.clusterService().state().getMetaData().getIndices().get(node.indices()[0]);
        Map<String, Object> mapping = node.mapping();
        if (indexMetaData == null){
            // TODO: should not happen!
            result.set(TaskResult.FAILURE);
            return;
        }

        try {
            MappingMetaData metaData = indexMetaData.getMappings().get(Constants.DEFAULT_MAPPING_TYPE);
            if (metaData == null) {
                // SHOULD NOT HAPPEN AS WELL
                result.set(TaskResult.FAILURE);
                return;
            }
            Map mergedMeta = (Map)metaData.getSourceAsMap().get("_meta");
            if (mergedMeta != null) {
                XContentHelper.update(mergedMeta, (Map) mapping.get("_meta"));
                mapping.put("_meta", mergedMeta);
            }
        } catch (IOException e) {
            result.setException(e);
        }

        PutMappingRequest request = new PutMappingRequest();
        request.indices(node.indices());
        request.type(Constants.DEFAULT_MAPPING_TYPE);
        request.source(mapping);
        putMappingAction.execute(request, new ActionListener<PutMappingResponse>() {
            @Override
            public void onResponse(PutMappingResponse response) {
                if(!response.isAcknowledged()){
                    warnNotAcknowledged("hui");
                }
                result.set(TaskResult.ONE_ROW);
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
    }
}
