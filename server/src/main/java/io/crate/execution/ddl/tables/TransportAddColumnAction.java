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

import io.crate.common.collections.Maps;
import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.execution.ddl.TransportSchemaUpdateAction;
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

    /**
     * Class for gathering mapping data in metadata compatible format.
     * AddColumnRequest has multiple columns and each of them can be a root of an object column tree and thus
     * can have many not-null/primary key/generated constraints.
     * We gather everything here in order to do only one column-tree traversal and call mergeInto only once for each type of constraint.
     */
    private final class ConvertedRequest {

        private final ArrayList<String> primaryKeys = new ArrayList<>();
        private final ArrayList<String> notNullColumns = new ArrayList<>();
        private final LinkedHashMap<String, String> generatedColumns = new LinkedHashMap<>();
        private final HashMap<String, Object> topColumns = new HashMap<>();

        public ConvertedRequest(AddColumnRequest request) {
            for (var column: request.columns()) {
                topColumns.put(column.name(), collectColumnInfo(column));
            }
        }

        /**
         * Returns properties map including all nested sub-columns if there any.
         * Fills constraints along the way - each leaf of the object columns tree might have a constraint on it.
         * Format of the top level properties field:
         * {
         *     col1: {
         *        position: some_position
         *        type: some_type
         *        ...
         *
         *        * optional, only for nested objects *
         *        properties: {
         *            nested_col1: {...},
         *            nested_col2: {...},
         *        }
         *     },
         *     col2: {...}
         * }
         */
        private HashMap<String, Object> collectColumnInfo(AddColumnRequest.StreamableColumnInfo columnInfo) {
            if (columnInfo.isPrimaryKey()) {
                primaryKeys.add(columnInfo.fqn());
            }
            if (columnInfo.isNullable()) {
                notNullColumns.add(columnInfo.fqn());
            }
            if (columnInfo.genExpression() != null) {
                generatedColumns.put(columnInfo.fqn(), columnInfo.genExpression());
            }
            HashMap<String, Object> propertiesMap = columnInfo.propertiesMap();

            if (columnInfo.children().isEmpty() == false) {
                HashMap<String, Object> nestedColumnsProperties = new HashMap<>();
                for (var column : columnInfo.children()) {
                    var nestedColumnMap = collectColumnInfo(column);
                    nestedColumnsProperties.put(column.name(), nestedColumnMap);
                }
                propertiesMap.put("properties", nestedColumnsProperties);
            }
            return propertiesMap;
        }
    }

    @Override
    public ClusterStateTaskExecutor<AddColumnRequest> clusterStateTaskExecutor(AddColumnRequest request) {
        return new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState, AddColumnRequest request) throws Exception {
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                ConvertedRequest requestConverter = new ConvertedRequest(request);

                if (request.isPartitioned()) {
                    String templateName = PartitionName.templateName(request.relationName().schema(), request.relationName().name());
                    IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);

                    Map<String, Object> existingTemplateMeta = new HashMap<>();

                    mergeDeltaIntoExistingMapping(existingTemplateMeta, request, requestConverter);

                    IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
                        indexTemplateMetadata,
                        existingTemplateMeta,
                        Collections.emptyMap(),
                        Settings.EMPTY,
                        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
                    );
                    metadataBuilder.put(newIndexTemplateMetadata);
                }

                currentState = updateMapping(currentState, metadataBuilder, request, requestConverter);
                // ensure the new table can still be parsed into a DocTableInfo to avoid breaking the table.
                new DocTableInfoFactory(nodeContext).create(request.relationName(), currentState);
                return currentState;
            }
        };
    }

    private ClusterState updateMapping(ClusterState currentState,
                                       Metadata.Builder metadataBuilder,
                                       AddColumnRequest request,
                                       ConvertedRequest convertedRequest) throws IOException {
        Index[] concreteIndices = resolveIndices(currentState, request.relationName().indexNameOrAlias());

        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            mergeDeltaIntoExistingMapping(indexMapping, request, convertedRequest);
            TransportSchemaUpdateAction.populateColumnPositions(indexMapping);

            MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);

            mapperService.merge(indexMapping, MapperService.MergeReason.MAPPING_UPDATE);
            DocumentMapper mapper = mapperService.documentMapper();

            IndexMetadata.Builder imBuilder = IndexMetadata.builder(indexMetadata);
            imBuilder.putMapping(new MappingMetadata(mapper.mappingSource())).mappingVersion(1 + imBuilder.mappingVersion());
            metadataBuilder.put(imBuilder); // implicitly increments metadata version.
        }

        return ClusterState.builder(currentState).metadata(metadataBuilder).build();
    }

    private static void mergeDeltaIntoExistingMapping(Map<String, Object> existingMapping,
                                                      AddColumnRequest request,
                                                      ConvertedRequest convertedRequest) {
        // _meta fields
        if (convertedRequest.primaryKeys.isEmpty() == false) {
            Maps.mergeInto(existingMapping, "_meta", List.of("primary_keys"), convertedRequest.primaryKeys,
                (map, key, value) -> ((ArrayList) map.computeIfAbsent(key, k -> new ArrayList<>())).addAll(convertedRequest.primaryKeys)
            );
        }
        if (convertedRequest.notNullColumns.isEmpty() == false) {
            Maps.mergeInto(existingMapping, "_meta", List.of("constraints", "not_null"), convertedRequest.notNullColumns,
                (map, key, value) -> {
                    if ("not_null".equals(key)) {
                        // we are merging on the end pf the path, i.e adding to the list
                        ((ArrayList) map.computeIfAbsent(key, k -> new ArrayList<>())).addAll(convertedRequest.notNullColumns);
                    } else {
                        // When adding not-null for the first time,
                        // Maps.mergeInto stops mid-path and transforms value (list) into chain of maps (to complement the remaining path) and adds value to the end.
                        // There is no existing constraints in this case so we can take value (convertedRequest.notNullColumns) as is.
                        map.put(key, value);
                    }

                }
            );
        }
        if (convertedRequest.generatedColumns.isEmpty() == false) {
            Maps.mergeInto(existingMapping, "_meta", List.of("generated_columns"), convertedRequest.generatedColumns,
                (map, key, value) -> ((LinkedHashMap) map.computeIfAbsent(key, k -> new LinkedHashMap<>())).putAll(convertedRequest.generatedColumns)
            );
        }
        for (var check: request.checkConstraints()) {
            Maps.mergeInto(existingMapping, "_meta", List.of("check_constraints"), check,
                (map, key, value) -> ((LinkedHashMap) map.computeIfAbsent(key, k -> new LinkedHashMap<>())).put(check.name(), check.expression())
            );
        }

        existingMapping.merge(
            "properties",
            convertedRequest.topColumns,
            (oldMap, newMap) -> {
                Maps.extendRecursive((Map<String, Object>) oldMap, (Map<String, Object>) newMap);
                return oldMap;
            }
        );
    }

    @Override
    public ClusterBlockException checkBlock(AddColumnRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
