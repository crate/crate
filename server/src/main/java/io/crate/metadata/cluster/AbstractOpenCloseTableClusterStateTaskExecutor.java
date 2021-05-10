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

import io.crate.execution.ddl.tables.OpenCloseTableOrPartitionRequest;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public abstract class AbstractOpenCloseTableClusterStateTaskExecutor extends DDLClusterStateTaskExecutor<OpenCloseTableOrPartitionRequest> {

    protected static class Context {

        private final Set<IndexMetadata> indicesMetadata;
        @Nullable
        private final IndexTemplateMetadata templateMetadata;
        @Nullable
        private final PartitionName partitionName;

        Context(Set<IndexMetadata> indicesMetadata, IndexTemplateMetadata templateMetadata, PartitionName partitionName) {
            this.indicesMetadata = indicesMetadata;
            this.templateMetadata = templateMetadata;
            this.partitionName = partitionName;
        }

        Set<IndexMetadata> indicesMetadata() {
            return indicesMetadata;
        }

        @Nullable
        IndexTemplateMetadata templateMetadata() {
            return templateMetadata;
        }

        @Nullable
        public PartitionName partitionName() {
            return partitionName;
        }
    }


    private final IndexNameExpressionResolver indexNameExpressionResolver;
    final AllocationService allocationService;
    final DDLClusterStateService ddlClusterStateService;

    AbstractOpenCloseTableClusterStateTaskExecutor(IndexNameExpressionResolver indexNameExpressionResolver,
                                                   AllocationService allocationService,
                                                   DDLClusterStateService ddlClusterStateService) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.allocationService = allocationService;
        this.ddlClusterStateService = ddlClusterStateService;
    }

    protected Context prepare(ClusterState currentState, OpenCloseTableOrPartitionRequest request) {
        RelationName relationName = request.tableIdent();
        String partitionIndexName = request.partitionIndexName();
        Metadata metadata = currentState.metadata();
        String indexToResolve = partitionIndexName != null ? partitionIndexName : relationName.indexNameOrAlias();
        PartitionName partitionName = partitionIndexName != null ? PartitionName.fromIndexOrTemplate(partitionIndexName) : null;
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
            currentState, IndicesOptions.lenientExpandOpen(), indexToResolve);
        Set<IndexMetadata> indicesMetadata = DDLClusterStateHelpers.indexMetadataSetFromIndexNames(metadata, concreteIndices, indexState());
        IndexTemplateMetadata indexTemplateMetadata = null;
        if (partitionIndexName == null) {
            indexTemplateMetadata = DDLClusterStateHelpers.templateMetadata(metadata, relationName);
        }
        return new Context(indicesMetadata, indexTemplateMetadata, partitionName);
    }

    protected abstract IndexMetadata.State indexState();

    static IndexTemplateMetadata updateOpenCloseOnPartitionTemplate(IndexTemplateMetadata indexTemplateMetadata,
                                                                    boolean openTable) {
        Map<String, Object> metaMap = Collections.singletonMap("_meta", Collections.singletonMap("closed", true));
        if (openTable) {
            //Remove the mapping from the template.
            return DDLClusterStateHelpers.updateTemplate(
                indexTemplateMetadata,
                Collections.emptyMap(),
                metaMap,
                Settings.EMPTY,
                (n, s) -> { },
                s -> true);
        } else {
            //Otherwise, add the mapping to the template.
            return DDLClusterStateHelpers.updateTemplate(
                indexTemplateMetadata,
                metaMap,
                Collections.emptyMap(),
                Settings.EMPTY,
                (n, s) -> { },
                s -> true);
        }
    }
}
