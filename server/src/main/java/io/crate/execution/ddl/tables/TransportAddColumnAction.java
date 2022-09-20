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

import io.crate.analyze.AnalyzedColumnDefinition;
import io.crate.common.collections.Maps;
import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.execution.ddl.TransportSchemaUpdateAction;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.cluster.DDLClusterStateHelpers;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.table.ColumnPolicies;
import io.crate.sql.tree.GenericProperties;
import io.crate.types.ObjectType;
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
import static org.elasticsearch.index.mapper.TypeParsers.DOC_VALUES;

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

                HashMap<String, Object> topColumns = new HashMap<>();
                for (var column: request.columns()) {
                    topColumns.put(column.name(), collectColumnProperties(column));
                }

                String templateName = PartitionName.templateName(request.relationName().schema(), request.relationName().name());
                IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);

                if (indexTemplateMetadata != null) {

                    Map<String, Object> existingTemplateMeta = new HashMap<>();

                    mergeDeltaIntoExistingMapping(existingTemplateMeta, request, topColumns);

                    IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
                        indexTemplateMetadata,
                        existingTemplateMeta,
                        Collections.emptyMap(),
                        Settings.EMPTY,
                        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
                    );
                    metadataBuilder.put(newIndexTemplateMetadata);
                }

                currentState = updateMapping(currentState, metadataBuilder, request, topColumns);
                // ensure the new table can still be parsed into a DocTableInfo to avoid breaking the table.
                new DocTableInfoFactory(nodeContext).create(request.relationName(), currentState);
                return currentState;
            }
        };
    }

    /**
     * Returns properties map including all nested sub-columns if there any.
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
     * Aligned with {@link AnalyzedColumnDefinition#toMapping(AnalyzedColumnDefinition)} )
     */
    private HashMap<String, Object> collectColumnProperties(AddColumnRequest.StreamableColumnInfo columnInfo) {
        HashMap<String, Object> properties = new HashMap<>();
        AnalyzedColumnDefinition.addTypeOptions(properties,
            columnInfo.type(),
            new GenericProperties<>(columnInfo.geoProperties()),
            columnInfo.geoTree(),
            columnInfo.analyzer()
        );
        properties.put("type",
            AnalyzedColumnDefinition.typeNameForESMapping(
                columnInfo.type(),
                columnInfo.analyzer(),
                columnInfo.indexType() == IndexType.FULLTEXT
            )
        );

        properties.put("position", columnInfo.position());

        if (columnInfo.indexType() == IndexType.NONE) {
            // we must use a boolean <p>false</p> and NO string "false", otherwise parser support for old indices will fail
            properties.put("index", false);
        }
        if (columnInfo.copyToTargets().isEmpty() == false) {
            properties.put("copy_to", columnInfo.copyToTargets());
        }

        if (columnInfo.isArrayType()) {
            HashMap<String, Object> outerMapping = new HashMap<>();
            outerMapping.put("type", "array");
            if (columnInfo.type().id() == ObjectType.ID) {
                objectMapping(properties, columnInfo);
            }
            outerMapping.put("inner", properties);
            return outerMapping;
        } else if (columnInfo.type().id() == ObjectType.ID) {
            objectMapping(properties, columnInfo);
        }

        if (columnInfo.hasDocValues() == false) {
            properties.put(DOC_VALUES, "false");
        }
        return properties;
    }

    private void objectMapping(Map<String, Object> propertiesMap, AddColumnRequest.StreamableColumnInfo columnInfo) {
        propertiesMap.put("dynamic", ColumnPolicies.encodeMappingValue(columnInfo.columnPolicy()));

        if (columnInfo.children().isEmpty() == false) {
            HashMap<String, Object> nestedColumnsProperties = new HashMap<>();
            for (var column : columnInfo.children()) {
                var nestedColumnMap = collectColumnProperties(column);
                nestedColumnsProperties.put(column.name(), nestedColumnMap);
            }
            propertiesMap.put("properties", nestedColumnsProperties);
        }
    }

    private ClusterState updateMapping(ClusterState currentState,
                                       Metadata.Builder metadataBuilder,
                                       AddColumnRequest request,
                                       HashMap<String, Object> topColumns) throws IOException {
        Index[] concreteIndices = resolveIndices(currentState, request.relationName().indexNameOrAlias());

        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            mergeDeltaIntoExistingMapping(indexMapping, request, topColumns);
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
                                                      HashMap<String, Object> topColumns) {

        for (var column: request.columns()) {
            Map<String, Object> meta = (Map<String, Object>) existingMapping.get("_meta");
            if (meta == null) {
                // existingMapping passed as an empty map in case of partitioned tables as template gets all changes unfiltered.
                // Create _meta so that we have a base to merge constraints.
                meta = new HashMap<>();
                existingMapping.put("_meta", meta);
            }

            // Check constraints are not bound to a column as column name is contained in expression itself (regardless of column or table level check).
            if (request.checkConstraints().isEmpty() == false) {
                Map<String, String> checkConstraints = (Map<String, String>) meta.get("check_constraints");
                if (checkConstraints == null) {
                    checkConstraints = new LinkedHashMap<>();
                    meta.put("check_constraints", checkConstraints);
                }
                for (AddColumnRequest.StreamableCheckConstraint checkConstraint: request.checkConstraints()) {
                    checkConstraints.put(checkConstraint.name(), checkConstraint.expression());
                }
            }

            // other constraints
            mergeConstraints(meta, column);

        }

        existingMapping.merge(
            "properties",
            topColumns,
            (oldMap, newMap) -> {
                Maps.extendRecursive((Map<String, Object>) oldMap, (Map<String, Object>) newMap);
                return oldMap;
            }
        );
    }

    private static void mergeConstraints(Map<String, Object> meta,
                                         AddColumnRequest.StreamableColumnInfo columnInfo) {
        List<String> primaryKeys = (List<String>) meta.get("primary_keys");

        Map<String, List<String>> constraints = (Map<String, List<String>>) meta.get("constraints");
        List<String> notNulls = constraints != null ? constraints.get("not_null") : null;

        Map<String, String> generatedColumns = (Map<String, String>) meta.get("generated_columns");

        if (columnInfo.isPrimaryKey()) {
            if (primaryKeys == null) {
                primaryKeys = new ArrayList<>();
                meta.put("primary_keys", primaryKeys);
            }
            primaryKeys.add(columnInfo.fqn());
        }

        if (columnInfo.isNullable()) {
            if (notNulls == null) {
                notNulls = new ArrayList<>();
                meta.put("constraints", Map.of("not_null", notNulls));
            }
            notNulls.add(columnInfo.fqn());
        }

        if (columnInfo.genExpression() != null) {
            if (generatedColumns == null) {
                generatedColumns = new LinkedHashMap<>();
                meta.put("generated_columns", generatedColumns);
            }
            generatedColumns.put(columnInfo.fqn(), columnInfo.genExpression());
        }

        if (columnInfo.children().isEmpty() == false) {
            for (AddColumnRequest.StreamableColumnInfo child: columnInfo.children()) {
                mergeConstraints(meta, child);
            }
        }
    }

    @Override
    public ClusterBlockException checkBlock(AddColumnRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
