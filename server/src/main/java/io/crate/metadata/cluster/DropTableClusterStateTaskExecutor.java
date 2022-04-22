/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.cluster;

import io.crate.execution.ddl.tables.DropTableRequest;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DropTableClusterStateTaskExecutor extends DDLClusterStateTaskExecutor<DropTableRequest> {

    private final MetadataDeleteIndexService deleteIndexService;
    private final DDLClusterStateService ddlClusterStateService;

    public DropTableClusterStateTaskExecutor(MetadataDeleteIndexService deleteIndexService,
                                             DDLClusterStateService ddlClusterStateService) {
        this.deleteIndexService = deleteIndexService;
        this.ddlClusterStateService = ddlClusterStateService;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, DropTableRequest request) throws Exception {
        RelationName relationName = request.tableIdent();
        final Set<Index> concreteIndices = new HashSet<>(Arrays.asList(IndexNameExpressionResolver.concreteIndices(
            currentState.metadata(), IndicesOptions.lenientExpandOpen(), relationName.indexNameOrAlias())));
        currentState = deleteIndexService.deleteIndices(currentState, concreteIndices);

        if (request.isPartitioned()) {
            // delete template
            String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
            Metadata.Builder metadata = Metadata.builder(currentState.metadata());
            metadata.removeTemplate(templateName);
            currentState = ClusterState.builder(currentState).metadata(metadata).build();
        }

        // call possible modifiers
        currentState = ddlClusterStateService.onDropTable(currentState, request.tableIdent());

        return currentState;
    }
}
