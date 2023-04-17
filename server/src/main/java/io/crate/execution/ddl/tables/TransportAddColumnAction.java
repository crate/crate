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

import static io.crate.metadata.Reference.buildTree;
import static io.crate.metadata.cluster.AlterTableClusterStateExecutor.resolveIndices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

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

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.collections.Maps;
import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.execution.ddl.TransportSchemaUpdateAction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.cluster.DDLClusterStateHelpers;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.table.ColumnPolicies;
import io.crate.types.ArrayType;
import io.crate.types.ObjectType;

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

                HashMap<ColumnIdent, List<Reference>> tree = buildTree(request.references());
                Map<String, Map<String, Object>> propertiesMap = buildMapping(null, tree);
                assert propertiesMap != null : "ADD COLUMN mapping can not be null"; // Only intermediate result can be null.

                String templateName = PartitionName.templateName(request.relationName().schema(), request.relationName().name());
                IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);

                if (indexTemplateMetadata != null) {
                    // Partitioned table

                    Map<String, Object> existingTemplateMeta = new HashMap<>();

                    mergeDeltaIntoExistingMapping(existingTemplateMeta, request, propertiesMap);

                    IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
                        indexTemplateMetadata,
                        existingTemplateMeta,
                        Collections.emptyMap(),
                        Settings.EMPTY,
                        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
                    );
                    metadataBuilder.put(newIndexTemplateMetadata);
                }

                currentState = updateMapping(currentState, metadataBuilder, request, propertiesMap);
                // ensure the new table can still be parsed into a DocTableInfo to avoid breaking the table.
                new DocTableInfoFactory(nodeContext).create(request.relationName(), currentState);
                return currentState;
            }
        };
    }


    private ClusterState updateMapping(ClusterState currentState,
                                       Metadata.Builder metadataBuilder,
                                       AddColumnRequest request,
                                       Map<String, Map<String, Object>> propertiesMap) throws IOException {
        Index[] concreteIndices = resolveIndices(currentState, request.relationName().indexNameOrAlias());

        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            mergeDeltaIntoExistingMapping(indexMapping, request, propertiesMap);
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
                                                      Map<String, Map<String, Object>> propertiesMap) {

        Map<String, Object> meta = (Map<String, Object>) existingMapping.get("_meta");
        if (meta == null) {
            // existingMapping passed as an empty map in case of partitioned tables as template gets all changes unfiltered.
            // Create _meta so that we have a base to merge constraints.
            meta = new HashMap<>();
            existingMapping.put("_meta", meta);
        }

        mergeConstraints(meta, request.references(), request.pKeyIndices(), request.checkConstraints());

        existingMapping.merge(
            "properties",
            propertiesMap,
            (oldMap, newMap) -> {
                Maps.extendRecursive((Map<String, Object>) oldMap, (Map<String, Object>) newMap);
                return oldMap;
            }
        );
    }

    private static void mergeConstraints(Map<String, Object> meta,
                                         List<Reference> references,
                                         IntArrayList pKeyIndices,
                                         Map<String, String> checkConstraints) {

        // CHECK
        if (checkConstraints.isEmpty() == false) {
            Map<String, String> existingCheckConstraints = (Map<String, String>) meta.get("check_constraints");
            if (existingCheckConstraints == null) {
                existingCheckConstraints = new HashMap<>();
                meta.put("check_constraints", existingCheckConstraints);
            }
            for (var entry : checkConstraints.entrySet()) {
                String name = entry.getKey();
                String expression = entry.getValue();
                existingCheckConstraints.put(name, expression);
            }
        }

        // PK
        if (pKeyIndices.isEmpty() == false) {
            List<String> primaryKeys = (List<String>) meta.get("primary_keys");
            if (primaryKeys == null) {
                primaryKeys = new ArrayList<>();
                meta.put("primary_keys", primaryKeys);
            }
            for (int i = 0; i < pKeyIndices.size(); i ++) {
                primaryKeys.add(references.get(pKeyIndices.get(i)).column().fqn());
            }
        }

        // Not nulls
        List<String> newNotNulls = references.stream().filter(ref -> !ref.isNullable()).map(ref -> ref.column().fqn()).collect(Collectors.toList());
        if (newNotNulls.isEmpty() == false) {
            Map<String, List<String>> constraints = (Map<String, List<String>>) meta.get("constraints");
            List<String> notNulls = constraints != null ? constraints.get("not_null") : null;
            if (notNulls == null) {
                notNulls = new ArrayList<>();
                var map = new HashMap<>();
                map.put("not_null", notNulls);
                meta.put("constraints", map);
            }
            notNulls.addAll(newNotNulls);
        }

        // Generated expressions
        List<GeneratedReference> newGenExpressions = references.stream()
            .filter(ref -> ref instanceof GeneratedReference)
            .map(ref -> (GeneratedReference) ref)
            .collect(Collectors.toList());
        if (newGenExpressions.isEmpty() == false) {
            Map<String, String> generatedColumns = (Map<String, String>) meta.get("generated_columns");
            if (generatedColumns == null) {
                generatedColumns = new HashMap<>();
                meta.put("generated_columns", generatedColumns);
            }
            for (GeneratedReference genRef: newGenExpressions) {
                generatedColumns.put(genRef.column().fqn(), genRef.formattedGeneratedExpression());
            }
        }

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

     */
    @Nullable
    private Map<String, Map<String, Object>> buildMapping(@Nullable ColumnIdent currentNode, HashMap<ColumnIdent, List<Reference>> tree) {
        List<Reference> children = tree.get(currentNode);
        if (children == null) {
            return null;
        }
        HashMap<String, Map<String, Object>> allColumnsMap = new HashMap<>();
        for (Reference child: children) {
            allColumnsMap.put(child.column().leafName(), addColumnProperties(child, tree));
        }
        return allColumnsMap;
    }


    /**
     * Aligned with AnalyzedColumnDefinition.toMapping()
     */
    private Map<String, Object> addColumnProperties(Reference reference, HashMap<ColumnIdent, List<Reference>> tree) {

        Map<String, Object> columnProperties = reference.toMapping();
        if (reference.valueType().id() == ArrayType.ID) {
            HashMap<String, Object> outerMapping = new HashMap<>();
            outerMapping.put("type", "array");
            if (ArrayType.unnest(reference.valueType()).id() == ObjectType.ID) {
                objectMapping(columnProperties, reference, tree);
            }
            outerMapping.put("inner", columnProperties);
            return outerMapping;
        } else if (reference.valueType().id() == ObjectType.ID) {
            objectMapping(columnProperties, reference, tree);
        }
        return columnProperties;
    }

    private void objectMapping(Map<String, Object> propertiesMap, Reference reference, HashMap<ColumnIdent, List<Reference>> tree) {
        propertiesMap.put("dynamic", ColumnPolicies.encodeMappingValue(reference.columnPolicy()));
        Map<String, Map<String, Object>> nestedObjectMap = buildMapping(reference.column(), tree);
        if (nestedObjectMap != null) {
            propertiesMap.put("properties", nestedObjectMap);
        }
    }

    @Override
    public ClusterBlockException checkBlock(AddColumnRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
