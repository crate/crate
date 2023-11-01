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

import static io.crate.execution.ddl.tables.MappingUtil.removeConstraints;
import static io.crate.execution.ddl.tables.MappingUtil.toProperties;
import static io.crate.metadata.Reference.buildTree;
import static io.crate.metadata.cluster.AlterTableClusterStateExecutor.resolveIndices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

import io.crate.analyze.AlterTableDropColumnAnalyzer;
import io.crate.analyze.AnalyzedAlterTableDropColumn.DropColumn;
import io.crate.common.CheckedFunction;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.execution.ddl.tables.MappingUtil.AllocPosition;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.cluster.DDLClusterStateHelpers;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;

public final class DropColumnTask extends DDLClusterStateTaskExecutor<DropColumnRequest> {
    private final NodeContext nodeContext;
    private final CheckedFunction<IndexMetadata, MapperService, IOException> createMapperService;

    public DropColumnTask(NodeContext nodeContext, CheckedFunction<IndexMetadata, MapperService, IOException> createMapperService) {
        this.nodeContext = nodeContext;
        this.createMapperService = createMapperService;
    }

    @Override
    public ClusterState execute(ClusterState currentState, DropColumnRequest request) throws Exception {
        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(nodeContext);
        DocTableInfo currentTable = docTableInfoFactory.create(request.relationName(), currentState);

        List<DropColumn> normalizedColumns = AlterTableDropColumnAnalyzer.validateDynamic(
            currentTable, normalizeColumns(request, currentTable));

        if (normalizedColumns.isEmpty()) {
            return currentState;
        }

        var refsToDrop = Lists2.map(normalizedColumns, dc -> dc.ref().withDropped(true));

        // Prepare columns tree, it will be used twice:
        // 1. For creating mapping for dropped columns.
        // 2. For creating "path-only, no properties" mapping to remove.
        List<Reference> references = new ArrayList<>(refsToDrop);
        for (var colToDrop : refsToDrop) {
            ColumnIdent parent = colToDrop.column();
            while ((parent = parent.getParent()) != null) {
                if (!Symbols.containsColumn(refsToDrop, parent)
                    && !Symbols.containsColumn(references, parent)) {
                    references.add(currentTable.getReference(parent));
                }
            }
        }
        // Add relevant constraints to be removed
        Set<String> constraintsToRemove = new HashSet<>();
        for (var constraint : currentTable.checkConstraints()) {
            Set<ColumnIdent> columnsInConstraint = new HashSet<>();
            RefVisitor.visitRefs(constraint.expression(), r -> columnsInConstraint.add(r.column()));

            if (columnsInConstraint.stream().allMatch(
                columnInConstraint -> {
                    for (var refToDrop : refsToDrop) {
                        if (columnInConstraint.equals(refToDrop.column()) || columnInConstraint.isChildOf(refToDrop.column())) {
                            return true;
                        }
                    }
                    return false;
                })) {
                constraintsToRemove.add(constraint.name());
            }
        }

        HashMap<ColumnIdent, List<Reference>> tree = buildTree(references);
        Map<String, Map<String, Object>> properties = toProperties(AllocPosition.forTable(currentTable), tree);
        Map<String, String> constraintsToRemoveAsMap = constraintsToRemove
            .stream().collect(Collectors.toMap(c -> c, c -> ""));
        Map<String, Object> mapping =
            Map.of("_meta", Map.of("check_constraints", constraintsToRemoveAsMap),
                "properties", properties);
        Map<String, Object> mappingToRemove =
            Map.of("_meta", Map.of("check_constraints", constraintsToRemoveAsMap),
                "properties", MappingUtil.toNamesOnlyProperties(tree));

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        String templateName = PartitionName.templateName(request.relationName().schema(), request.relationName().name());
        IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);
        if (indexTemplateMetadata != null) {
            // Partitioned table
            IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
                indexTemplateMetadata,
                mapping,
                mappingToRemove,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
            );
            metadataBuilder.put(newIndexTemplateMetadata);
        }

        currentState = updateMapping(
            currentState,
            metadataBuilder,
            request.relationName().indexNameOrAlias(),
            properties,
            mappingToRemove,
            constraintsToRemove
        );
        // ensure the table can still be parsed into a DocTableInfo to avoid breaking the table.
        docTableInfoFactory.create(request.relationName(), currentState);
        return currentState;
    }


    /**
     * Replace existing references by taking them from the cluster state (so that we get an actual version with assigned OID).
     */
    static List<DropColumn> normalizeColumns(DropColumnRequest request, DocTableInfo currentTable) {
        List<DropColumn> dropColumns = new ArrayList<>();

        // Get existing references
        for (var colToDrop : request.colsToDrop()) {
            var col = colToDrop.ref().column();
            Reference ref = currentTable.getReference(col);
            if (ref == null || ref.isDropped()) {
                if (colToDrop.ifExists() == false) {
                    throw new ColumnUnknownException(col, currentTable.ident());
                }
            } else {
                dropColumns.add(new DropColumn(ref, colToDrop.ifExists())); // Actually a column to drop, Get updated reference from the cluster state.
            }
        }
        return dropColumns;
    }

    private ClusterState updateMapping(ClusterState currentState,
                                       Metadata.Builder metadataBuilder,
                                       String indexName,
                                       Map<String, Map<String, Object>> propertiesMap,
                                       Map<String, Object> mappingToRemove,
                                       Set<String> constraintsToRemove) throws IOException {
        Index[] concreteIndices = resolveIndices(currentState, indexName);

        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            removeConstraints(indexMapping, constraintsToRemove);

            mergeDeltaIntoExistingMapping(indexMapping, propertiesMap);
            indexMapping = DDLClusterStateHelpers.removeFromMapping(indexMapping, mappingToRemove);

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
                                                      Map<String, Map<String, Object>> propertiesMap) {

        Map<String, Object> meta = (Map<String, Object>) existingMapping.get("_meta");
        if (meta == null) {
            // existingMapping passed as an empty map in case of partitioned tables as template gets all changes unfiltered.
            // Create _meta so that we have a base to merge constraints.
            meta = new HashMap<>();
            existingMapping.put("_meta", meta);
        }

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
