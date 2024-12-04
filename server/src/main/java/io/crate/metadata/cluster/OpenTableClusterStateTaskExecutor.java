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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;

import io.crate.execution.ddl.tables.OpenTableRequest;
import io.crate.execution.ddl.tables.TransportCloseTable;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;


public class OpenTableClusterStateTaskExecutor extends DDLClusterStateTaskExecutor<OpenTableRequest> {

    private record Context(Set<IndexMetadata> indicesMetadata, IndexTemplateMetadata templateMetadata, PartitionName partitionName) {
    }

    private final AllocationService allocationService;
    private final DDLClusterStateService ddlClusterStateService;
    private final MetadataIndexUpgradeService metadataIndexUpgradeService;
    private final IndicesService indicesService;

    public OpenTableClusterStateTaskExecutor(AllocationService allocationService,
                                             DDLClusterStateService ddlClusterStateService,
                                             MetadataIndexUpgradeService metadataIndexUpgradeService,
                                             IndicesService indexServices) {
        this.allocationService = allocationService;
        this.ddlClusterStateService = ddlClusterStateService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        this.indicesService = indexServices;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, OpenTableRequest request) throws Exception {
        Context context = prepare(currentState, request);
        Set<IndexMetadata> indicesToOpen = context.indicesMetadata();
        IndexTemplateMetadata templateMetadata = context.templateMetadata();

        if (indicesToOpen.isEmpty() && templateMetadata == null) {
            return currentState;
        }

        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());
        final Version minIndexCompatibilityVersion = currentState.nodes().getMaxNodeVersion()
            .minimumIndexCompatibilityVersion();
        for (IndexMetadata closedMetadata : indicesToOpen) {
            final String indexName = closedMetadata.getIndex().getName();
            blocksBuilder.removeIndexBlockWithId(indexName, TransportCloseTable.INDEX_CLOSED_BLOCK_ID);

            if (closedMetadata.getState() == IndexMetadata.State.OPEN) {
                continue;
            }
            final Settings.Builder updatedSettings = Settings.builder().put(closedMetadata.getSettings());
            updatedSettings.remove(IndexMetadata.VERIFIED_BEFORE_CLOSE_SETTING.getKey());

            IndexMetadata updatedIndexMetadata = IndexMetadata.builder(closedMetadata)
                .state(IndexMetadata.State.OPEN)
                .settingsVersion(closedMetadata.getSettingsVersion() + 1)
                .settings(updatedSettings)
                .build();

            // The index might be closed because we couldn't import it due to old incompatible version
            // We need to check that this index can be upgraded to the current version
            updatedIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(updatedIndexMetadata, templateMetadata, minIndexCompatibilityVersion);
            try {
                indicesService.verifyIndexMetadata(updatedIndexMetadata, updatedIndexMetadata);
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to verify index " + indexName, e);
            }

            mdBuilder.put(updatedIndexMetadata, true);
        }

        // remove closed flag at possible partitioned table template
        if (templateMetadata != null) {
            mdBuilder.put(updateOpenCloseOnPartitionTemplate(templateMetadata));
        }

        // The Metadata will always be overridden (and not merged!) when applying it on a cluster state builder.
        // So we must re-build the state with the latest modifications before we pass this state to possible modifiers.
        // Otherwise they would operate on the old Metadata and would just ignore any modifications.
        ClusterState updatedState = ClusterState.builder(currentState).metadata(mdBuilder).blocks(blocksBuilder).build();

        // call possible registered modifiers
        if (context.partitionName() != null) {
            updatedState = ddlClusterStateService.onOpenTablePartition(updatedState, context.partitionName());
        } else {
            updatedState = ddlClusterStateService.onOpenTable(updatedState, request.relation());
        }

        RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable());
        for (IndexMetadata index : indicesToOpen) {
            rtBuilder.addAsFromCloseToOpen(updatedState.metadata().getIndexSafe(index.getIndex()));
        }

        //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
        return allocationService.reroute(
            ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
            "indices opened " + indicesToOpen);
    }

    private Context prepare(ClusterState currentState, OpenTableRequest request) {
        RelationName relationName = request.relation();
        List<String> partitionValues = request.partitionValues();
        PartitionName partitionName = partitionValues.isEmpty() ? null : new PartitionName(relationName, partitionValues);
        Metadata metadata = currentState.metadata();
        String[] concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
            currentState.metadata(),
            IndicesOptions.LENIENT_EXPAND_OPEN,
            partitionName == null ? relationName.indexNameOrAlias() : partitionName.asIndexName()
        );
        Set<IndexMetadata> indicesMetadata = DDLClusterStateHelpers.indexMetadataSetFromIndexNames(metadata, concreteIndices, IndexMetadata.State.OPEN);
        IndexTemplateMetadata indexTemplateMetadata = null;
        if (partitionName == null) {
            indexTemplateMetadata = DDLClusterStateHelpers.templateMetadata(metadata, relationName);
        }
        return new Context(indicesMetadata, indexTemplateMetadata, partitionName);
    }

    private static IndexTemplateMetadata updateOpenCloseOnPartitionTemplate(IndexTemplateMetadata indexTemplateMetadata) {
        Map<String, Object> metaMap = Collections.singletonMap("_meta", Collections.singletonMap("closed", true));
        //Remove the mapping from the template.
        return DDLClusterStateHelpers.updateTemplate(
            indexTemplateMetadata,
            Collections.emptyMap(),
            metaMap,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
        );
    }

}
