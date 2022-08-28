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

import io.crate.analyze.AnalyzedTableElements;
import io.crate.common.collections.Maps;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.crate.metadata.cluster.AlterTableClusterStateExecutor.resolveIndices;

@Singleton
public class TransportAddColumnAction extends AbstractDDLTransportAction<AddColumnRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/add_column";
    private final NodeContext nodeContext;
    private final IndicesService indicesService;

    @Inject
    public TransportAddColumnAction(TransportService transportService,
                                    ClusterService clusterService,
                                    IndicesService indicesService,
                                    ThreadPool threadPool,
                                    NodeContext nodeContext) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            AddColumnRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "add-column");
        this.nodeContext = nodeContext;
        this.indicesService = indicesService;
    }

    @Override
    public ClusterStateTaskExecutor<AddColumnRequest> clusterStateTaskExecutor(AddColumnRequest request) {
        return new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState, AddColumnRequest request) throws Exception {
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                if (request.isPartitioned()) {
                    String templateName = PartitionName.templateName(request.relationName().schema(), request.relationName().name());
                    IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);

                    Map<String, Object> existingTemplateMeta = new HashMap<>();
                    mergeDeltaIntoExistingMapping(existingTemplateMeta, request);

                    IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
                        indexTemplateMetadata,
                        existingTemplateMeta,
                        Collections.emptyMap(),
                        Settings.EMPTY,
                        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
                    );
                    metadataBuilder.put(newIndexTemplateMetadata);
                }

                currentState = updateMapping(currentState, request, metadataBuilder);
                // ensure the new table can still be parsed into a DocTableInfo to avoid breaking the table.
                new DocTableInfoFactory(nodeContext).create(request.relationName(), currentState);
                return currentState;
            }
        };
    }

    /**
     * Structure of the metadata is aligned with {@link AnalyzedTableElements#toMapping(AnalyzedTableElements)}
     */
    private ClusterState updateMapping(ClusterState currentState,
                                       AddColumnRequest request,
                                       Metadata.Builder metadataBuilder) throws IOException {
        Index[] concreteIndices = resolveIndices(currentState, request.relationName().indexNameOrAlias());

        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            mergeDeltaIntoExistingMapping(indexMapping, request);

            MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);

            // Add current mapping and initialize DocumentMapper, so that mapperService.documentMapper()
            // is not null. It's needed to trigger ColumnPositionResolver and publish cluster state with non-negative calculated column positions
            mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);

            mapperService.merge(indexMapping, MapperService.MergeReason.MAPPING_UPDATE);
            DocumentMapper mapper = mapperService.documentMapper();

            IndexMetadata.Builder imBuilder = IndexMetadata.builder(indexMetadata);
            imBuilder.putMapping(new MappingMetadata(mapper.mappingSource())).mappingVersion(1 + imBuilder.mappingVersion());
            metadataBuilder.put(imBuilder); // implicitly increments metadata version.
        }

        return ClusterState.builder(currentState).metadata(metadataBuilder).build();
    }

    private static void mergeDeltaIntoExistingMapping(Map<String, Object> existingMapping, AddColumnRequest request) {
        // _meta fields
        if (request.isPrimaryKey()) {
            Maps.mergeInto(existingMapping, "_meta", List.of("primary_keys"), request.columnRef().column().fqn(),
                (map, key, value) -> ((ArrayList) map.computeIfAbsent(key, k -> new ArrayList<>())).add(value)
            );
        }
        if (request.columnRef().isNullable() == false) {
            Maps.mergeInto(existingMapping, "_meta", List.of("constraints", "not_null"), request.columnRef().column().fqn(),
                (map, key, value) -> ((ArrayList) map.computeIfAbsent(key, k -> new ArrayList<>())).add(value)
            );
        }
        if (request.generatedExpression() != null) {
            Maps.mergeInto(existingMapping, "_meta", List.of("generated_columns"), Map.entry(request.columnRef().column().fqn(), request.generatedExpression()),
                (map, key, value) -> {
                    Map.Entry<String, String> nameAndExpression = (Map.Entry) value;
                    ((LinkedHashMap) map.computeIfAbsent(key, k -> new LinkedHashMap<>())).put(nameAndExpression.getKey(), nameAndExpression.getValue());
                }
            );
        }
        for (AddColumnRequest.StreamableCheckConstraint checkConstraint: request.checkConstraints()) {
            Maps.mergeInto(existingMapping, "_meta", List.of("check_constraints"), checkConstraint,
                (map, key, value) -> {
                    AddColumnRequest.StreamableCheckConstraint constraint = (AddColumnRequest.StreamableCheckConstraint) value;
                    ((LinkedHashMap) map.computeIfAbsent(key, k -> new LinkedHashMap<>())).put(constraint.name(), constraint.expression());
                }
            );
        }

        /*
        properties field (contains position, columnstore flag, index flag and column type specific properties).
        In case of the nested object 'properties' field is repeated,
        and thus 'a.b.c' object col path is 'properties.a.properties.b.properties.c'

        format:
        {
            col1: {
               position: some_position
               type: some_type
               ...

               * optional, only for nested objects *
               properties: {
                   nested_col1: {...},
                   nested_col2: {...},
               }
            },
            col2: {...}
        }
        */

        Map currentProperties = (Map) existingMapping.get("properties");
        if (currentProperties == null) {
            // existingMapping passed as an empty map in case of partitioned tables as template gets all changes unfiltered.
            // Nothing to merge to the empty map, just put all incoming mappings according to the format specified above.
            var props = new LinkedHashMap<String, Object>();
            props.put(request.columnRef().column().name(), request.columnProperties());
            existingMapping.put("properties", props);
        } else {
            currentProperties.merge(
                // in case of adding a nested object, say, a.b.c,
                // actual column being added is c (column().leafName()) and full name is column.fqn().
                // However, we intentionally use outermost object column 'a' returned by column().name()
                // so that we don't need to compute overlap of (existingMap, request.columnProperties()) and mutate request.columnProperties() to exclude overlap.
                // We insert request.columnProperties() which contains correct chain with all nested maps
                // but we need to keep existing sibling sub-cols (say, a.b1, a.b.c1) on each level and thus we use extendRecursive.
                request.columnRef().column().name(),
                request.columnProperties(),
                (oldMap, newMap) -> {
                    Maps.extendRecursive((Map<String, Object>) oldMap, (Map<String, Object>) newMap);
                    return oldMap;
                }
            );
        }
    }

    @Override
    public ClusterBlockException checkBlock(AddColumnRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
