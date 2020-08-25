/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.doc;

import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.crate.blob.v2.BlobIndex;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.metadata.view.ViewsMetadata;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * SchemaInfo for all user tables.
 *
 * <p>
 * Can be used to retrieve DocTableInfo's of tables in the `doc` or a custom schema.
 * </p>
 *
 * <p>
 *     See the following table for examples how the indexName is encoded.
 *     Functions to encode/decode are either in {@link RelationName} or {@link PartitionName}
 * </p>
 *
 * <table>
 *     <tr>
 *         <th>schema</th>
 *         <th>tableName</th>
 *         <th>indices</th>
 *         <th>partitioned</th>
 *         <th>templateName</th>
 *     </tr>
 *
 *     <tr>
 *         <td>doc</td>
 *         <td>t1</td>
 *         <td>[ t1 ]</td>
 *         <td>NO</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>doc</td>
 *         <td>t1p</td>
 *         <td>[ .partitioned.t1p.&lt;ident&gt; ]</td>
 *         <td>YES</td>
 *         <td>.partitioned.t1p.</td>
 *     </tr>
 *     <tr>
 *         <td>custom</td>
 *         <td>t1</td>
 *         <td>[ custom.t1 ]</td>
 *         <td>NO</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>custom</td>
 *         <td>t1p</td>
 *         <td>[ custom..partitioned.t1p.&lt;ident&gt; ]</td>
 *         <td>YES</td>
 *         <td>custom..partitioned.t1p.</td>
 *     </tr>
 * </table>
 */
public class DocSchemaInfo implements SchemaInfo {

    public static final String NAME = "doc";

    private final ClusterService clusterService;
    private final DocTableInfoFactory docTableInfoFactory;
    private final ViewInfoFactory viewInfoFactory;
    private final NodeContext nodeCtx;
    private final UserDefinedFunctionService udfService;

    private final ConcurrentHashMap<String, DocTableInfo> docTableByName = new ConcurrentHashMap<>();

    private static final Predicate<String> NO_BLOB_NOR_DANGLING =
        index -> ! (BlobIndex.isBlobIndex(index) || IndexParts.isDangling(index));

    private final String schemaName;

    /**
     * DocSchemaInfo constructor for the all schemas.
     */
    public DocSchemaInfo(final String schemaName,
                         ClusterService clusterService,
                         NodeContext nodeCtx,
                         UserDefinedFunctionService udfService,
                         ViewInfoFactory viewInfoFactory,
                         DocTableInfoFactory docTableInfoFactory) {
        this.nodeCtx = nodeCtx;
        this.schemaName = schemaName;
        this.clusterService = clusterService;
        this.udfService = udfService;
        this.viewInfoFactory = viewInfoFactory;
        this.docTableInfoFactory = docTableInfoFactory;
    }

    @Override
    public TableInfo getTableInfo(String name) {
        try {
            return docTableByName.computeIfAbsent(name, n -> docTableInfoFactory.create(new RelationName(schemaName, n), clusterService.state()));
        } catch (ResourceUnknownException e) {
            return null;
        }
    }

    private Collection<String> tableNames() {
        Set<String> tables = new HashSet<>();
        extractRelationNamesForSchema(Stream.of(clusterService.state().metadata().getConcreteAllIndices()),
            schemaName, tables);

        // Search for partitioned table templates
        Iterator<String> templates = clusterService.state().metadata().getTemplates().keysIt();
        while (templates.hasNext()) {
            String templateName = templates.next();
            if (!IndexParts.isPartitioned(templateName)) {
                continue;
            }
            try {
                PartitionName partitionName = PartitionName.fromIndexOrTemplate(templateName);
                RelationName ti = partitionName.relationName();
                if (schemaName.equalsIgnoreCase(ti.schema())) {
                    tables.add(ti.name());
                }
            } catch (IllegalArgumentException e) {
                // do nothing
            }
        }

        return tables;
    }

    @Nullable
    private ViewInfo getViewInfo(String name) {
        return viewInfoFactory.create(new RelationName(schemaName, name), clusterService.state());
    }

    private Collection<String> viewNames() {
        ViewsMetadata viewMetadata = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
        if (viewMetadata == null) {
            return Collections.emptySet();
        }
        Set<String> views = new HashSet<>();
        extractRelationNamesForSchema(StreamSupport.stream(viewMetadata.names().spliterator(), false),
            schemaName, views);
        return views;
    }

    private static void extractRelationNamesForSchema(Stream<String> stream, String schema, Set<String> target) {
        stream.filter(NO_BLOB_NOR_DANGLING)
            .map(IndexParts::new)
            .filter(indexParts -> !indexParts.isPartitioned())
            .filter(indexParts -> indexParts.matchesSchema(schema))
            .map(IndexParts::getTable)
            .forEach(target::add);
    }

    @Override
    public String name() {
        return schemaName;
    }

    @Override
    public void invalidateTableCache(String tableName) {
        docTableByName.remove(tableName);
    }

    @Override
    public void update(ClusterChangedEvent event) {
        assert event.metadataChanged() : "metadataChanged must be true if update is called";

        // search for aliases of deleted and created indices, they must be invalidated also
        Metadata prevMetadata = event.previousState().metadata();
        for (Index index : event.indicesDeleted()) {
            invalidateFromIndex(index, prevMetadata);
        }
        Metadata newMetadata = event.state().metadata();
        for (String index : event.indicesCreated()) {
            invalidateAliases(newMetadata.index(index).getAliases());
        }

        // search for templates with changed meta data => invalidate template aliases
        ImmutableOpenMap<String, IndexTemplateMetadata> newTemplates = newMetadata.templates();
        ImmutableOpenMap<String, IndexTemplateMetadata> prevTemplates = prevMetadata.templates();
        if (!newTemplates.equals(prevTemplates)) {
            for (ObjectCursor<IndexTemplateMetadata> cursor : newTemplates.values()) {
                invalidateAliases(cursor.value.aliases());
            }
            for (ObjectCursor<IndexTemplateMetadata> cursor : prevTemplates.values()) {
                invalidateAliases(cursor.value.aliases());
            }
        }

        // search indices with changed meta data
        Iterator<String> currentTablesIt = docTableByName.keySet().iterator();
        ObjectLookupContainer<String> templates = newTemplates.keys();
        ImmutableOpenMap<String, IndexMetadata> indices = newMetadata.indices();
        while (currentTablesIt.hasNext()) {
            String tableName = currentTablesIt.next();
            String indexName = getIndexName(tableName);

            IndexMetadata newIndexMetadata = newMetadata.index(indexName);
            if (newIndexMetadata == null) {
                docTableByName.remove(tableName);
            } else {
                IndexMetadata oldIndexMetadata = prevMetadata.index(indexName);
                if (oldIndexMetadata != null && ClusterChangedEvent.indexMetadataChanged(oldIndexMetadata, newIndexMetadata)) {
                    docTableByName.remove(tableName);
                    // invalidate aliases of changed indices
                    invalidateAliases(newIndexMetadata.getAliases());
                    invalidateAliases(oldIndexMetadata.getAliases());
                } else {
                    // this is the case if a single partition has been modified using alter table <t> partition (...)
                    String possibleTemplateName = PartitionName.templateName(name(), tableName);
                    if (templates.contains(possibleTemplateName)) {
                        for (ObjectObjectCursor<String, IndexMetadata> indexEntry : indices) {
                            if (IndexParts.isPartitioned(indexEntry.key)) {
                                docTableByName.remove(tableName);
                                break;
                            }
                        }
                    }
                }
            }
        }

        // re register UDFs for this schema
        UserDefinedFunctionsMetadata udfMetadata = newMetadata.custom(UserDefinedFunctionsMetadata.TYPE);
        if (udfMetadata != null) {
            udfService.updateImplementations(
                schemaName,
                udfMetadata.functionsMetadata().stream().filter(f -> schemaName.equals(f.schema())));
        }
    }

    /**
     * checks if metadata contains a particular index and
     * invalidates its aliases if so
     */
    @VisibleForTesting
    void invalidateFromIndex(Index index, Metadata metadata) {
        IndexMetadata indexMetadata = metadata.index(index);
        if (indexMetadata != null) {
            invalidateAliases(indexMetadata.getAliases());
        }
    }

    private String getIndexName(String tableName) {
        if (schemaName.equalsIgnoreCase(Schemas.DOC_SCHEMA_NAME)) {
            return tableName;
        } else {
            return schemaName + "." + tableName;
        }
    }

    private void invalidateAliases(ImmutableOpenMap<String, AliasMetadata> aliases) {
        assert aliases != null : "aliases must not be null";
        if (aliases.size() > 0) {
            aliases.keysIt().forEachRemaining(docTableByName::remove);
        }
    }

    @Override
    public String toString() {
        return "DocSchemaInfo(" + name() + ")";
    }

    @Override
    public Iterable<TableInfo> getTables() {
        return tableNames().stream()
            .map(this::getTableInfo)
            .filter(Objects::nonNull)
            ::iterator;
    }

    @Override
    public Iterable<ViewInfo> getViews() {
        return viewNames().stream()
            .map(this::getViewInfo)
            .filter(Objects::nonNull)
            ::iterator;
    }

    @Override
    public void close() throws Exception {
        nodeCtx.functions().deregisterUdfResolversForSchema(schemaName);
    }
}
