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

import static io.crate.execution.ddl.tables.MappingUtil.createMapping;
import static io.crate.execution.ddl.tables.MappingUtil.mergeConstraints;
import static io.crate.metadata.cluster.AlterTableClusterStateExecutor.resolveIndices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;

import io.crate.common.CheckedFunction;
import io.crate.common.collections.Maps;
import io.crate.execution.ddl.TransportSchemaUpdateAction;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.cluster.DDLClusterStateHelpers;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;

public final class AddColumnTask extends DDLClusterStateTaskExecutor<AddColumnRequest> {

    private final NodeContext nodeContext;
    private final CheckedFunction<IndexMetadata, MapperService, IOException> createMapperService;

    public AddColumnTask(NodeContext nodeContext, CheckedFunction<IndexMetadata, MapperService, IOException> createMapperService) {
        this.nodeContext = nodeContext;
        this.createMapperService = createMapperService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClusterState execute(ClusterState currentState, AddColumnRequest request) throws Exception {
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(nodeContext);
        DocTableInfo currentTable = docTableInfoFactory.create(request.relationName(), currentState);

        boolean allExist = true;
        for (Reference ref : request.references()) {
            Reference exists = currentTable.getReference(ref.column());
            if (exists == null) {
                allExist = false;
            } else if (ref.valueType().id() != exists.valueType().id()) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Column `%s` already exists with type `%s`. Cannot add same column with type `%s`",
                    ref.column(),
                    exists.valueType().getName(),
                    ref.valueType().getName()));
            }
        }
        if (allExist) {
            return currentState;
        }

        request = addMissingParentColumns(request, currentTable);

        Map<String, Object> mapping = createMapping(
            request.references(),
            request.pKeyIndices(),
            request.checkConstraints(),
            Map.of(),
            List.of(),
            null,
            null
        );

        String templateName = PartitionName.templateName(request.relationName().schema(), request.relationName().name());
        IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);
        if (indexTemplateMetadata != null) {
            // Partitioned table
            IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
                indexTemplateMetadata,
                mapping,
                Collections.emptyMap(),
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
            );
            metadataBuilder.put(newIndexTemplateMetadata);
        }

        currentState = updateMapping(currentState, metadataBuilder, request, (Map<String, Map<String, Object>>) mapping.get("properties"));
        // ensure the new table can still be parsed into a DocTableInfo to avoid breaking the table.
        docTableInfoFactory.create(request.relationName(), currentState);
        return currentState;
    }

    static AddColumnRequest addMissingParentColumns(AddColumnRequest request, DocTableInfo currentTable) {
        List<Reference> newColumns = new ArrayList<>();
        boolean anyMissing = false;
        for (var newColumn : request.references()) {
            if (newColumn.column().isTopLevel()) {
                newColumns.add(newColumn);
            } else {
                ColumnIdent parent = newColumn.column();
                while ((parent = parent.getParent()) != null) {
                    if (!Symbols.containsColumn(request.references(), parent)
                            && !Symbols.containsColumn(newColumns, parent)) {
                        newColumns.add(currentTable.getReference(parent));
                        anyMissing = true;
                    }
                }
                newColumns.add(newColumn);
            }
        }
        if (anyMissing) {
            return new AddColumnRequest(
                request.relationName(),
                newColumns,
                request.checkConstraints(),
                request.pKeyIndices()
            );
        } else {
            return request;
        }
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

            MapperService mapperService = createMapperService.apply(indexMetadata);

            DocumentMapper mapper = mapperService.merge(indexMapping, MapperService.MergeReason.MAPPING_UPDATE);

            IndexMetadata.Builder imBuilder = IndexMetadata.builder(indexMetadata);
            imBuilder.putMapping(new MappingMetadata(mapper.mappingSource())).mappingVersion(1 + imBuilder.mappingVersion());

            metadataBuilder.put(imBuilder); // implicitly increments metadata version.
        }

        return ClusterState.builder(currentState).metadata(metadataBuilder).build();
    }

    @SuppressWarnings("unchecked")
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



}
