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
import io.crate.execution.ddl.tables.RenameTableRequest;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocIndexMetaData;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexTemplateMissingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RenameTableClusterStateExecutor extends DDLClusterStateTaskExecutor<RenameTableRequest> {

    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);

    private final Logger logger;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final MetaDataIndexAliasesService metaDataIndexAliasesService;
    private final NamedXContentRegistry xContentRegistry;
    private final DDLClusterStateService ddlClusterStateService;

    public RenameTableClusterStateExecutor(Settings settings,
                                           IndexNameExpressionResolver indexNameExpressionResolver,
                                           MetaDataIndexAliasesService metaDataIndexAliasesService,
                                           NamedXContentRegistry namedXContentRegistry,
                                           DDLClusterStateService ddlClusterStateService) {
        logger = Loggers.getLogger(getClass(), settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.metaDataIndexAliasesService = metaDataIndexAliasesService;
        this.xContentRegistry = namedXContentRegistry;
        this.ddlClusterStateService = ddlClusterStateService;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, RenameTableRequest request) throws Exception {
        RelationName sourceRelationName = request.sourceTableIdent();
        RelationName targetRelationName = request.targetTableIdent();
        boolean isPartitioned = request.isPartitioned();

        MetaData.Builder mdBuilder = null;
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
            .blocks(currentState.blocks());

        IndexTemplateMetaData indexTemplateMetaData = null;
        if (isPartitioned) {
            indexTemplateMetaData = DDLClusterStateHelpers.templateMetaData(currentState.getMetaData(), sourceRelationName);
            if (indexTemplateMetaData == null) {
                throw new IndexTemplateMissingException("Template for partitioned table is missing");
            }
            validatePartitionedTemplateIsMarkedAsClosed(indexTemplateMetaData, sourceRelationName);
        }

        logger.info("renaming table '{}' to '{}'", sourceRelationName.fqn(), targetRelationName.fqn());

        try {
            Index[] sourceIndices = indexNameExpressionResolver.concreteIndices(currentState, STRICT_INDICES_OPTIONS,
                sourceRelationName.indexName());
            validateAllIndicesAreClosed(currentState, sourceRelationName, sourceIndices, isPartitioned);

            String[] targetIndices;
            if (isPartitioned) {
                targetIndices = buildNewPartitionIndexNames(sourceIndices, targetRelationName);
                // change the alias for all partitions
                currentState = changeAliases(currentState, sourceIndices, sourceRelationName.indexName(),
                    targetRelationName.indexName());
            } else {
                targetIndices = new String[]{targetRelationName.indexName()};
            }

            MetaData metaData = currentState.getMetaData();
            mdBuilder = MetaData.builder(metaData);

            // rename all concrete indices
            renameIndices(metaData, mdBuilder, blocksBuilder, sourceIndices, targetIndices);

        } catch (IndexNotFoundException e) {
            if (isPartitioned == false) {
                throw e;
            }
            // empty partition case, no indices, just a template exists
        }

        if (isPartitioned) {
            // rename template (delete old one, and create a new one)
            if (mdBuilder == null) {
                // partition is empty, only template exists
                mdBuilder = MetaData.builder(currentState.getMetaData());
            }
            renameTemplate(mdBuilder, indexTemplateMetaData, targetRelationName);
        }
        // The MetaData will always be overridden (and not merged!) when applying it on a cluster state builder.
        // So we must re-build the state with the latest modifications before we pass this state to possible modifiers.
        // Otherwise they would operate on the old MetaData and would just ignore any modifications.
        currentState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

        // call possible modifiers
        return ddlClusterStateService.onRenameTable(currentState, sourceRelationName, targetRelationName, request.isPartitioned());
    }

    private ClusterState changeAliases(ClusterState currentState, Index[] partitions, String oldAlias, String newAlias) {
        List<AliasAction> aliasActions = new ArrayList<>(2 * partitions.length);
        for (Index partition : partitions) {
            String indexName = partition.getName();
            aliasActions.add(new AliasAction.Remove(indexName, oldAlias));
            aliasActions.add(new AliasAction.Add(indexName, newAlias, null, null, null));
        }
        return metaDataIndexAliasesService.innerExecute(currentState, aliasActions);
    }

    private void validatePartitionedTemplateIsMarkedAsClosed(IndexTemplateMetaData templateMetaData,
                                                             RelationName relationName) throws Exception {
        Map<String, Object> mapping = parseMapping(
            templateMetaData.getMappings().get(Constants.DEFAULT_MAPPING_TYPE).string());
        //noinspection unchecked
        mapping = (Map<String, Object>) mapping.get(Constants.DEFAULT_MAPPING_TYPE);
        assert mapping != null : "parsed mapping must not be null";

        if (DocIndexMetaData.isClosed(null, mapping, true) == false) {
            throw new IllegalStateException("Partitioned table '" + relationName.fqn() +
                                            "' is not closed, cannot perform a rename");
        }
    }

    private Map<String, Object> parseMapping(String mappingSource) throws Exception {
        try (XContentParser parser = XContentFactory.xContent(mappingSource).createParser(xContentRegistry, mappingSource)) {
            return parser.map();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse mapping", e);
        }
    }

    private static void renameIndices(MetaData metaData,
                                      MetaData.Builder mdBuilder,
                                      ClusterBlocks.Builder blocksBuilder,
                                      Index[] sourceIndices,
                                      String[] targetIndices) {
        for (int i = 0; i < sourceIndices.length; i++) {
            Index index = sourceIndices[i];
            IndexMetaData indexMetaData = metaData.getIndexSafe(index);
            IndexMetaData targetIndexMetadata = IndexMetaData.builder(indexMetaData)
                .index(targetIndices[i]).build();
            mdBuilder.remove(index.getName());
            mdBuilder.put(targetIndexMetadata, true);
            blocksBuilder.removeIndexBlocks(index.getName());
            blocksBuilder.addBlocks(targetIndexMetadata);
        }
    }

    private static void renameTemplate(MetaData.Builder mdBuilder,
                                       IndexTemplateMetaData sourceTemplateMetaData,
                                       RelationName targetIdent) throws Exception {
        String targetTemplateName = PartitionName.templateName(targetIdent.schema(), targetIdent.name());
        String targetTemplatePrefix = PartitionName.templatePrefix(targetIdent.schema(), targetIdent.name());

        IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData.builder(targetTemplateName)
            .order(sourceTemplateMetaData.order())
            .settings(sourceTemplateMetaData.settings())
            .patterns(Collections.singletonList(targetTemplatePrefix))
            .putMapping(Constants.DEFAULT_MAPPING_TYPE, sourceTemplateMetaData.mappings().get(Constants.DEFAULT_MAPPING_TYPE))
            .putAlias(AliasMetaData.builder(targetIdent.indexName()));
        IndexTemplateMetaData newTemplate = templateBuilder.build();

        mdBuilder.removeTemplate(sourceTemplateMetaData.getName()).put(newTemplate);
    }

    private static String[] buildNewPartitionIndexNames(Index[] sourceIndices, RelationName targetRelationName) {
        String[] newPartitionIndexNames = new String[sourceIndices.length];
        for (int i = 0; i < sourceIndices.length; i++) {
            PartitionName partitionName = PartitionName.fromIndexOrTemplate(sourceIndices[i].getName());
            String targetIndexName = IndexParts.toIndexName(targetRelationName, partitionName.ident());
            newPartitionIndexNames[i] = targetIndexName;
        }
        return newPartitionIndexNames;
    }

    private static void validateAllIndicesAreClosed(ClusterState currentState,
                                                    RelationName sourceRelationName,
                                                    Index[] sourceIndices,
                                                    boolean isPartitioned) {
        for (Index index : sourceIndices) {
            final IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
            if (indexMetaData.getState() != IndexMetaData.State.CLOSE) {
                String msg = "Table '" + sourceRelationName.fqn() + "' is not closed, cannot perform a rename";
                if (isPartitioned) {
                    msg = "Partition '" + index.getName() + "' of table '" + sourceRelationName.fqn() +
                          "' is not closed, cannot perform a rename";
                }
                throw new IllegalStateException(msg);
            }
        }
    }
}
