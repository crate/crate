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

package io.crate.execution.ddl.tables;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.cluster.DDLClusterStateHelpers;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.metadata.doc.DocTableInfoFactory;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.List;

import static io.crate.execution.ddl.tables.MappingUtil.removeConstraints;
import static io.crate.metadata.cluster.AlterTableClusterStateExecutor.resolveIndices;

@Singleton
public class TransportDropConstraintAction extends AbstractDDLTransportAction<DropConstraintRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/drop_constraint";
    private final NodeContext nodeContext;
    private final IndicesService indicesService;

    @Inject
    public TransportDropConstraintAction(TransportService transportService,
                                    ClusterService clusterService,
                                    IndicesService indicesService,
                                    ThreadPool threadPool,
                                    NodeContext nodeContext) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            DropConstraintRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "drop-constraint");
        this.nodeContext = nodeContext;
        this.indicesService = indicesService;
    }


    @Override
    public ClusterStateTaskExecutor<DropConstraintRequest> clusterStateTaskExecutor(DropConstraintRequest request) {
        return new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState, DropConstraintRequest request) throws Exception {
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                boolean updated = false;

                // Update template if table is partitioned
                String templateName = PartitionName.templateName(request.relationName().schema(), request.relationName().name());
                IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);

                if (indexTemplateMetadata != null) {
                    // Removal happens by key, value in the deepest map is not used.
                    Map<String, Object> mapToRemove = Map.of("_meta", Map.of("check_constraints", Map.of(request.constraintName(), "")));

                    IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
                        indexTemplateMetadata,
                        Collections.emptyMap(),
                        mapToRemove,
                        Settings.EMPTY,
                        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
                    );
                    updated = true;
                    metadataBuilder.put(newIndexTemplateMetadata);
                }

                updated |= updateMapping(currentState, metadataBuilder, request);
                if (updated) {
                    // ensure the new table can still be parsed into a DocTableInfo to avoid breaking the table.
                    new DocTableInfoFactory(nodeContext).create(request.relationName(), currentState);
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }
                return currentState;
            }
        };
    }

    private boolean updateMapping(ClusterState currentState,
                                  Metadata.Builder metadataBuilder,
                                  DropConstraintRequest request) throws IOException {
        Index[] concreteIndices = resolveIndices(currentState, request.relationName().indexNameOrAlias());
        boolean updated = false;

        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            Map<String, Object> currentSource = indexMetadata.mapping().sourceAsMap();

            updated |= removeConstraints(currentSource, List.of(request.constraintName()));

            MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);

            mapperService.merge(currentSource, MapperService.MergeReason.MAPPING_UPDATE);
            DocumentMapper mapper = mapperService.documentMapper();

            IndexMetadata.Builder imBuilder = IndexMetadata.builder(indexMetadata);
            imBuilder.putMapping(new MappingMetadata(mapper.mappingSource())).mappingVersion(1 + imBuilder.mappingVersion());
            metadataBuilder.put(imBuilder); // implicitly increments metadata version.
        }
        return updated;
    }

    @Override
    public ClusterBlockException checkBlock(DropConstraintRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
