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

package io.crate.metadata.doc;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.blob.v2.BlobIndex;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.replication.logical.metadata.PublicationsMetadata;

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

    private final ConcurrentHashMap<String, DocTableInfo> docTableByName = new ConcurrentHashMap<>();

    public static final Predicate<String> NO_BLOB_NOR_DANGLING =
        index -> ! (BlobIndex.isBlobIndex(index) || IndexName.isDangling(index));

    private final String schemaName;

    /**
     * DocSchemaInfo constructor for the all schemas.
     */
    public DocSchemaInfo(final String schemaName,
                         ClusterService clusterService,
                         ViewInfoFactory viewInfoFactory,
                         DocTableInfoFactory docTableInfoFactory) {
        this.schemaName = schemaName;
        this.clusterService = clusterService;
        this.viewInfoFactory = viewInfoFactory;
        this.docTableInfoFactory = docTableInfoFactory;
    }

    @Override
    public TableInfo getTableInfo(String name) {
        Metadata metadata = clusterService.state().metadata();
        DocTableInfo docTableInfo = docTableByName.get(name);
        try {
            RelationName relation = new RelationName(schemaName, name);
            if (docTableInfo == null) {
                return docTableByName.computeIfAbsent(
                    name,
                    n -> docTableInfoFactory.create(relation, metadata)
                );
            }
            if (docTableInfo.tableVersion() < getTableVersion(metadata, relation)) {
                DocTableInfo newTable = docTableInfoFactory.create(new RelationName(schemaName, name), metadata);
                docTableByName.replace(name, newTable);
                return newTable;
            }
            return docTableInfo;
        } catch (Exception e) {
            if (e instanceof ResourceUnknownException) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public DocTableInfo create(RelationName relationName, Metadata metadata) {
        return docTableInfoFactory.create(relationName, metadata);
    }

    private static long getTableVersion(Metadata metadata, RelationName name) {
        RelationMetadata.Table table = metadata.getRelation(name);
        if (table == null) {
            throw new RelationUnknown(name);
        }
        return table.tableVersion();
    }

    private Collection<String> tableNames() {
        List<RelationMetadata.Table> relations = clusterService.state().metadata().relations(schemaName, RelationMetadata.Table.class);
        return relations.stream().map(r -> r.name().name()).toList();
    }

    @Nullable
    @Override
    public ViewInfo getViewInfo(String name) {
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
            .map(IndexName::decode)
            .filter(indexParts -> !indexParts.isPartitioned())
            .filter(indexParts -> indexParts.schema().equals(schema))
            .map(IndexParts::table)
            .forEach(target::add);
    }

    @Override
    public String name() {
        return schemaName;
    }

    @Override
    public void update(ClusterChangedEvent event) {
        assert event.metadataChanged() : "metadataChanged must be true if update is called";

        Metadata prevMetadata = event.previousState().metadata();
        Metadata newMetadata = event.state().metadata();

        // search indices with changed relations
        Iterator<String> currentTablesIt = docTableByName.keySet().iterator();
        while (currentTablesIt.hasNext()) {
            String tableName = currentTablesIt.next();
            RelationMetadata newRelationMetadata = newMetadata.getRelation(new RelationName(schemaName, tableName));
            if (newRelationMetadata == null) {
                docTableByName.remove(tableName);
            } else {
                RelationMetadata oldRelationMetadata = prevMetadata.getRelation(new RelationName(schemaName, tableName));
                if (newRelationMetadata.equals(oldRelationMetadata) == false) {
                    docTableByName.remove(tableName);
                }
            }
        }

        PublicationsMetadata prevPublicationsMetadata = prevMetadata.custom(PublicationsMetadata.TYPE);
        PublicationsMetadata newPublicationsMetadata = newMetadata.custom(PublicationsMetadata.TYPE);
        var tablesAffectedByPublicationsChange = getTablesAffectedByPublicationsChange(prevPublicationsMetadata,
                                                                                       newPublicationsMetadata,
                                                                                       docTableByName);
        for (String updatedTable : tablesAffectedByPublicationsChange) {
            docTableByName.remove(updatedTable);
        }
    }

    @VisibleForTesting
    static Set<String> getTablesAffectedByPublicationsChange(
        @Nullable PublicationsMetadata prevMetadata,
        @Nullable PublicationsMetadata newMetadata,
        Map<String, DocTableInfo> docTableByName) {

        if (Objects.equals(prevMetadata, newMetadata)) {
            return Set.of();
        }

        if (prevMetadata == null) {
            // No previous publications exist so all tables have to be updated which are now published
            var result = new HashSet<String>();
            for (var publication : newMetadata.publications().values()) {
                if (publication.isForAllTables()) {
                    return docTableByName.keySet();
                } else {
                    for (var table : publication.tables()) {
                        result.add(table.name());
                    }
                }
            }
            return result;
        }

        // Find the difference of tables which have been published across all publications
        var prevPublishedTables = new HashSet<String>();
        var newPublishedTables = new HashSet<String>();
        var allPrevTablesArePublished = false;
        var allNewTablesArePublished = false;

        for (var publication : prevMetadata.publications().values()) {
            if (publication.isForAllTables()) {
                allPrevTablesArePublished = true;
            } else {
                for (var table : publication.tables()) {
                    prevPublishedTables.add(table.name());
                }
            }
        }

        for (var publication : newMetadata.publications().values()) {
            if (publication.isForAllTables()) {
                allNewTablesArePublished = true;
            } else {
                for (var table : publication.tables()) {
                    newPublishedTables.add(table.name());
                }
            }
        }

        if (allPrevTablesArePublished == true && allNewTablesArePublished == true) {
            // Nothing to update, all tables are still published
            return Set.of();
        } else if (allPrevTablesArePublished == true && allNewTablesArePublished == false) {
            // Update all tables which are not published anymore
            var result = docTableByName.keySet();
            result.removeAll(newPublishedTables);
            return result;
        } else if (allPrevTablesArePublished == false && allNewTablesArePublished == true) {
            // Update all tables which have not been published
            var result = docTableByName.keySet();
            result.removeAll(prevPublishedTables);
            return result;
        } else {
            // Update all tables where the state has changed
            var result = new HashSet<String>();
            result.addAll(prevPublishedTables);
            result.addAll(newPublishedTables);
            var intersection = new HashSet<>(prevPublishedTables);
            intersection.retainAll(newPublishedTables);
            result.removeAll(intersection);
            return result;
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
    }
}
