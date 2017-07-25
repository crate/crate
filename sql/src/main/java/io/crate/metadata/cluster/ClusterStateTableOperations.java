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

import io.crate.Constants;
import io.crate.executor.transport.AlterTableOperation;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.MetaDataIndexStateService.INDEX_CLOSED_BLOCK;

@Singleton
public class ClusterStateTableOperations {

    private final Logger logger;
    private final AllocationService allocationService;
    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;
    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public ClusterStateTableOperations(Settings settings,
                                       AllocationService allocationService,
                                       MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                                       IndicesService indexServices,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        this.allocationService = allocationService;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        this.indicesService = indexServices;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        logger = Loggers.getLogger(getClass(), settings);
    }

    public ClusterState closeTableOrPartition(ClusterState currentState,
                                              TableIdent tableIdent,
                                              @Nullable String partitionIndexName) {
        MetaData metaData = currentState.metaData();
        String indexToResolve = partitionIndexName != null ? partitionIndexName : tableIdent.indexName();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
            currentState, IndicesOptions.lenientExpandOpen(), indexToResolve);
        Set<IndexMetaData> indicesToClose = indexMetaDataSetFromIndexNames(metaData, concreteIndices, IndexMetaData.State.CLOSE);
        IndexTemplateMetaData indexTemplateMetaData = null;
        if (partitionIndexName == null) {
            indexTemplateMetaData = templateMetaData(metaData, tableIdent);
        }

        if (indicesToClose.isEmpty() && indexTemplateMetaData == null) {
            return currentState;
        }

        RestoreService.checkIndexClosing(currentState, indicesToClose);
        // Check if index closing conflicts with any running snapshots
        SnapshotsService.checkIndexClosing(currentState, indicesToClose);

        if (logger.isInfoEnabled()) {
            if (partitionIndexName != null) {
                logger.info("closing partition [{}] of table [{}]", partitionIndexName, tableIdent.fqn());
            } else if (concreteIndices.length > 1) {
                logger.info("closing partitioned table [{}] including all partitions", tableIdent.fqn());
            } else {
                logger.info("closing table [{}]", tableIdent.fqn());
            }
        }

        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());
        for (IndexMetaData openIndexMetadata : indicesToClose) {
            final String indexName = openIndexMetadata.getIndex().getName();
            mdBuilder.put(IndexMetaData.builder(openIndexMetadata).state(IndexMetaData.State.CLOSE));
            blocksBuilder.addIndexBlock(indexName, INDEX_CLOSED_BLOCK);
        }

        // update possible partitioned table template
        if (indexTemplateMetaData != null) {
            mdBuilder.put(updateOpenCloseOnPartitionTemplate(indexTemplateMetaData, false));
        }

        ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

        RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
        for (IndexMetaData index : indicesToClose) {
            rtBuilder.remove(index.getIndex().getName());
        }

        //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
        return allocationService.reroute(
            ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
            "indices closed " + indicesToClose);
    }

    public ClusterState openTableOrPartition(ClusterState currentState,
                                             TableIdent tableIdent,
                                             @Nullable String partitionIndexName) {
        MetaData metaData = currentState.metaData();
        String indexToResolve = partitionIndexName != null ? partitionIndexName : tableIdent.indexName();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
            currentState, IndicesOptions.lenientExpandOpen(), indexToResolve);
        Set<IndexMetaData> indicesToOpen = indexMetaDataSetFromIndexNames(metaData, concreteIndices, IndexMetaData.State.OPEN);
        IndexTemplateMetaData indexTemplateMetaData = null;
        if (partitionIndexName == null) {
            indexTemplateMetaData = templateMetaData(metaData, tableIdent);
        }

        if (indicesToOpen.isEmpty() && indexTemplateMetaData == null) {
            return currentState;
        }

        if (logger.isInfoEnabled()) {
            if (partitionIndexName != null) {
                logger.info("opening partition [{}] of table [{}]", partitionIndexName, tableIdent.fqn());
            } else if (concreteIndices.length > 1) {
                logger.info("opening partitioned table [{}] including all partitions", tableIdent.fqn());
            } else {
                logger.info("opening table [{}]", tableIdent.fqn());
            }
        }

        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());
        final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
            .minimumIndexCompatibilityVersion();
        for (IndexMetaData closedMetaData : indicesToOpen) {
            final String indexName = closedMetaData.getIndex().getName();
            IndexMetaData indexMetaData = IndexMetaData.builder(closedMetaData).state(IndexMetaData.State.OPEN).build();
            // The index might be closed because we couldn't import it due to old incompatible version
            // We need to check that this index can be upgraded to the current version
            indexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData, minIndexCompatibilityVersion);
            try {
                indicesService.verifyIndexMetadata(indexMetaData, indexMetaData);
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to verify index " + indexMetaData.getIndex(), e);
            }

            mdBuilder.put(indexMetaData, true);
            blocksBuilder.removeIndexBlock(indexName, INDEX_CLOSED_BLOCK);
        }

        // update possible partitioned table template
        if (indexTemplateMetaData != null) {
            mdBuilder.put(updateOpenCloseOnPartitionTemplate(indexTemplateMetaData, true));
        }

        ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

        RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable());
        for (IndexMetaData index : indicesToOpen) {
            rtBuilder.addAsFromCloseToOpen(updatedState.metaData().getIndexSafe(index.getIndex()));
        }

        //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
        return allocationService.reroute(
            ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
            "indices opened " + indicesToOpen);
    }

    private static IndexTemplateMetaData updateOpenCloseOnPartitionTemplate(IndexTemplateMetaData indexTemplateMetaData,
                                                                            boolean openTable) {
        Map<String, Object> metaMap = Collections.singletonMap("_meta", Collections.singletonMap("closed", true));
        if (openTable) {
            //Remove the mapping from the template.
            return updateTemplate(indexTemplateMetaData, Collections.emptyMap(), metaMap, Settings.EMPTY);
        } else {
            //Otherwise, add the mapping to the template.
            return updateTemplate(indexTemplateMetaData, metaMap, Collections.emptyMap(), Settings.EMPTY);
        }
    }

    private static IndexTemplateMetaData updateTemplate(IndexTemplateMetaData indexTemplateMetaData,
                                                        Map<String, Object> newMappings,
                                                        Map<String, Object> mappingsToRemove,
                                                        Settings newSettings) {

        // merge mappings
        Map<String, Object> mapping = AlterTableOperation.mergeTemplateMapping(indexTemplateMetaData, newMappings);

        // remove mappings
        mapping = AlterTableOperation.removeFromMapping(mapping, mappingsToRemove);

        // merge settings
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(indexTemplateMetaData.settings());
        settingsBuilder.put(newSettings);

        // wrap it in a type map if its not
        if (mapping.size() != 1 || mapping.containsKey(Constants.DEFAULT_MAPPING_TYPE) == false) {
            mapping = MapBuilder.<String, Object>newMapBuilder().put(Constants.DEFAULT_MAPPING_TYPE, mapping).map();
        }
        try {
            return new IndexTemplateMetaData.Builder(indexTemplateMetaData)
                .settings(settingsBuilder)
                .putMapping(Constants.DEFAULT_MAPPING_TYPE, XContentFactory.jsonBuilder().map(mapping).string())
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<IndexMetaData> indexMetaDataSetFromIndexNames(MetaData metaData,
                                                                     String[] indices,
                                                                     IndexMetaData.State state) {
        Set<IndexMetaData> indicesMetaData = new HashSet<>();
        for (String indexName : indices) {
            IndexMetaData indexMetaData = metaData.index(indexName);
            if (indexMetaData != null && indexMetaData.getState() != state) {
                indicesMetaData.add(indexMetaData);
            }
        }
        return indicesMetaData;
    }

    @Nullable
    private static IndexTemplateMetaData templateMetaData(MetaData metaData, TableIdent tableIdent) {
        String templateName = PartitionName.templateName(tableIdent.schema(), tableIdent.name());
        return metaData.templates().get(templateName);
    }
}
