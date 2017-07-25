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

import io.crate.executor.transport.ddl.OpenCloseTableOrPartitionRequest;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public abstract class AbstractOpenCloseTableClusterStateTaskExecutor extends DDLClusterStateTaskExecutor<OpenCloseTableOrPartitionRequest> {

    protected static class Context {

        private final Set<IndexMetaData> indicesMetaData;
        @Nullable
        private final IndexTemplateMetaData templateMetaData;
        @Nullable
        private final PartitionName partitionName;

        Context(Set<IndexMetaData> indicesMetaData, IndexTemplateMetaData templateMetaData, PartitionName partitionName) {
            this.indicesMetaData = indicesMetaData;
            this.templateMetaData = templateMetaData;
            this.partitionName = partitionName;
        }

        Set<IndexMetaData> indicesMetaData() {
            return indicesMetaData;
        }

        @Nullable
        IndexTemplateMetaData templateMetaData() {
            return templateMetaData;
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
        TableIdent tableIdent = request.tableIdent();
        String partitionIndexName = request.partitionIndexName();
        MetaData metaData = currentState.metaData();
        String indexToResolve = partitionIndexName != null ? partitionIndexName : tableIdent.indexName();
        PartitionName partitionName = partitionIndexName != null ? PartitionName.fromIndexOrTemplate(partitionIndexName) : null;
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
            currentState, IndicesOptions.lenientExpandOpen(), indexToResolve);
        Set<IndexMetaData> indicesMetaData = DDLClusterStateHelpers.indexMetaDataSetFromIndexNames(metaData, concreteIndices, indexState());
        IndexTemplateMetaData indexTemplateMetaData = null;
        if (partitionIndexName == null) {
            indexTemplateMetaData = DDLClusterStateHelpers.templateMetaData(metaData, tableIdent);
        }
        return new Context(indicesMetaData, indexTemplateMetaData, partitionName);
    }

    protected abstract IndexMetaData.State indexState();

    static IndexTemplateMetaData updateOpenCloseOnPartitionTemplate(IndexTemplateMetaData indexTemplateMetaData,
                                                                    boolean openTable) {
        Map<String, Object> metaMap = Collections.singletonMap("_meta", Collections.singletonMap("closed", true));
        if (openTable) {
            //Remove the mapping from the template.
            return DDLClusterStateHelpers.updateTemplate(indexTemplateMetaData, Collections.emptyMap(), metaMap, Settings.EMPTY);
        } else {
            //Otherwise, add the mapping to the template.
            return DDLClusterStateHelpers.updateTemplate(indexTemplateMetaData, metaMap, Collections.emptyMap(), Settings.EMPTY);
        }
    }
}
