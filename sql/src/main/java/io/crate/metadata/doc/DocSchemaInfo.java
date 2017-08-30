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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import io.crate.blob.v2.BlobIndex;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.operation.udf.UserDefinedFunctionsMetaData;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * SchemaInfo for all user tables.
 *
 * <p>
 * Can be used to retrieve DocTableInfo's of tables in the `doc` or a custom schema.
 * </p>
 *
 * <p>
 *     See the following table for examples how the indexName is encoded.
 *     Functions to encode/decode are either in {@link TableIdent} or {@link PartitionName}
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
    private final Functions functions;
    private final UserDefinedFunctionService udfService;

    private final ConcurrentHashMap<String, DocTableInfo> docTableByName = new ConcurrentHashMap<>();

    private static final Predicate<String> NO_BLOB = ((Predicate<String>)BlobIndex::isBlobIndex).negate();

    private final String schemaName;

    /**
     * DocSchemaInfo constructor for the all schemas.
     */
    public DocSchemaInfo(final String schemaName,
                         ClusterService clusterService,
                         Functions functions,
                         UserDefinedFunctionService udfService,
                         DocTableInfoFactory docTableInfoFactory) {
        this.functions = functions;
        this.schemaName = schemaName;
        this.clusterService = clusterService;
        this.udfService = udfService;
        this.docTableInfoFactory = docTableInfoFactory;
    }

    private DocTableInfo innerGetTableInfo(String tableName) {
        return docTableInfoFactory.create(new TableIdent(schemaName, tableName), clusterService);
    }

    @Override
    public DocTableInfo getTableInfo(String name) {
        try {
            return docTableByName.computeIfAbsent(name, this::innerGetTableInfo);
        } catch (ResourceUnknownException e) {
            return null;
        }
    }

    private Collection<String> tableNames() {
        Set<String> tables = new HashSet<>();

        Stream.of(clusterService.state().metaData().getConcreteAllIndices())
            .filter(NO_BLOB)
            .map(IndexParts::new)
            .filter(indexParts -> !indexParts.isPartitioned())
            .filter(indexParts -> indexParts.matchesSchema(schemaName))
            .map(IndexParts::getTable)
            .forEach(tables::add);

        // Search for partitioned table templates
        Iterator<String> templates = clusterService.state().metaData().getTemplates().keysIt();
        while (templates.hasNext()) {
            String templateName = templates.next();
            if (!IndexParts.isPartitioned(templateName)) {
                continue;
            }
            try {
                PartitionName partitionName = PartitionName.fromIndexOrTemplate(templateName);
                TableIdent ti = partitionName.tableIdent();
                if (schemaName.equalsIgnoreCase(ti.schema())) {
                    tables.add(ti.name());
                }
            } catch (IllegalArgumentException e) {
                // do nothing
            }
        }

        return tables;
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
        assert event.metaDataChanged() : "metaDataChanged must be true if update is called";

        // search for aliases of deleted and created indices, they must be invalidated also
        MetaData prevMetaData = event.previousState().metaData();
        for (Index index : event.indicesDeleted()) {
            invalidateFromIndex(index, prevMetaData);
        }
        MetaData newMetaData = event.state().metaData();
        for (String index : event.indicesCreated()) {
            invalidateAliases(newMetaData.index(index).getAliases());
        }

        // search for templates with changed meta data => invalidate template aliases
        ImmutableOpenMap<String, IndexTemplateMetaData> newTemplates = newMetaData.templates();
        ImmutableOpenMap<String, IndexTemplateMetaData> prevTemplates = prevMetaData.templates();
        if (!newTemplates.equals(prevTemplates)) {
            for (ObjectCursor<IndexTemplateMetaData> cursor : newTemplates.values()) {
                invalidateAliases(cursor.value.aliases());
            }
            for (ObjectCursor<IndexTemplateMetaData> cursor : prevTemplates.values()) {
                invalidateAliases(cursor.value.aliases());
            }
        }

        // search indices with changed meta data
        Iterator<String> currentTablesIt = docTableByName.keySet().iterator();
        ObjectLookupContainer<String> templates = newTemplates.keys();
        ImmutableOpenMap<String, IndexMetaData> indices = newMetaData.indices();
        while (currentTablesIt.hasNext()) {
            String tableName = currentTablesIt.next();
            String indexName = getIndexName(tableName);

            IndexMetaData newIndexMetaData = newMetaData.index(indexName);
            if (newIndexMetaData == null) {
                docTableByName.remove(tableName);
            } else {
                IndexMetaData oldIndexMetaData = prevMetaData.index(indexName);
                if (oldIndexMetaData != null && ClusterChangedEvent.indexMetaDataChanged(oldIndexMetaData, newIndexMetaData)) {
                    docTableByName.remove(tableName);
                    // invalidate aliases of changed indices
                    invalidateAliases(newIndexMetaData.getAliases());
                    invalidateAliases(oldIndexMetaData.getAliases());
                } else {
                    // this is the case if a single partition has been modified using alter table <t> partition (...)
                    String possibleTemplateName = PartitionName.templateName(name(), tableName);
                    if (templates.contains(possibleTemplateName)) {
                        for (ObjectObjectCursor<String, IndexMetaData> indexEntry : indices) {
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
        UserDefinedFunctionsMetaData udfMetaData = newMetaData.custom(UserDefinedFunctionsMetaData.TYPE);
        if (udfMetaData != null) {
            udfService.updateImplementations(
                schemaName,
                udfMetaData.functionsMetaData().stream().filter(f -> schemaName.equals(f.schema())));
        }
    }

    /**
     * checks if metaData contains a particular index and
     * invalidates its aliases if so
     */
    @VisibleForTesting
    void invalidateFromIndex(Index index, MetaData metaData) {
        IndexMetaData indexMetaData = metaData.index(index);
        if (indexMetaData != null) {
            invalidateAliases(indexMetaData.getAliases());
        }
    }

    private String getIndexName(String tableName) {
        if (schemaName.equalsIgnoreCase(Schemas.DOC_SCHEMA_NAME)) {
            return tableName;
        } else {
            return schemaName + "." + tableName;
        }
    }

    private void invalidateAliases(ImmutableOpenMap<String, AliasMetaData> aliases) {
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
    public Iterator<TableInfo> iterator() {
        return Iterators.transform(tableNames().iterator(), this::getTableInfo);
    }

    @Override
    public void close() throws Exception {
        functions.deregisterUdfResolversForSchema(schemaName);
    }
}
