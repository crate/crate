/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.cluster;

import io.crate.executor.transport.ddl.DropTableRequest;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DropTableClusterStateTaskExecutor extends DDLClusterStateTaskExecutor<DropTableRequest> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final MetaDataDeleteIndexService deleteIndexService;
    private final DDLClusterStateService ddlClusterStateService;

    public DropTableClusterStateTaskExecutor(IndexNameExpressionResolver indexNameExpressionResolver,
                                             MetaDataDeleteIndexService deleteIndexService,
                                             DDLClusterStateService ddlClusterStateService) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.deleteIndexService = deleteIndexService;
        this.ddlClusterStateService = ddlClusterStateService;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, DropTableRequest request) throws Exception {
        TableIdent tableIdent = request.tableIdent();
        final Set<Index> concreteIndices = new HashSet<>(Arrays.asList(indexNameExpressionResolver.concreteIndices(
            currentState, IndicesOptions.lenientExpandOpen(), tableIdent.indexName())));
        currentState = deleteIndexService.deleteIndices(currentState, concreteIndices);

        if (request.isPartitioned()) {
            // delete template
            String templateName = PartitionName.templateName(tableIdent.schema(), tableIdent.name());
            MetaData.Builder metaData = MetaData.builder(currentState.metaData());
            metaData.removeTemplate(templateName);
            currentState = ClusterState.builder(currentState).metaData(metaData).build();
        }

        // call possible modifiers
        currentState = ddlClusterStateService.onDropTable(currentState, request.tableIdent());

        return currentState;
    }
}
