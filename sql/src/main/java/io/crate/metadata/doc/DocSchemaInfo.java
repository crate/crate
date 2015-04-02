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
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;

public class DocSchemaInfo implements SchemaInfo, ClusterStateListener {

    private final ClusterService clusterService;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;

    private final static Predicate<String> DOC_SCHEMA_TABLES_FILTER = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
            //noinspection SimplifiableIfStatement
            if (BlobIndices.isBlobIndex(input)) {
                return false;
            }
            return !ReferenceInfos.SCHEMA_PATTERN.matcher(input).matches();
        }
    };

    private final Predicate<String> tablesFilter;

    private final LoadingCache<String, DocTableInfo> cache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .build(
                    new CacheLoader<String, DocTableInfo>() {
                        @Override
                        public DocTableInfo load(@Nonnull String key) throws Exception {
                            return innerGetTableInfo(key);
                        }
                    }
            );

    private final Function<String, TableInfo> tableInfoFunction;
    private final String schemaName;


    /**
     * DocSchemaInfo constructor for the default (doc) schema.
     */
    @Inject
    public DocSchemaInfo(ClusterService clusterService,
                         TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        schemaName = ReferenceInfos.DEFAULT_SCHEMA_NAME;
        this.clusterService = clusterService;
        clusterService.add(this);
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;

        this.tablesFilter = DOC_SCHEMA_TABLES_FILTER;
        this.tableInfoFunction = new Function<String, TableInfo>() {
            @Nullable
            @Override
            public TableInfo apply(String input) {
                return getTableInfo(input);
            }
        };
    }

    /**
     * constructor used for custom schemas
     */
    public DocSchemaInfo(final String schemaName,
                         ClusterService clusterService,
                         TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        this.schemaName = schemaName;
        this.clusterService = clusterService;
        clusterService.add(this);
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;

        this.tableInfoFunction = new Function<String, TableInfo>() {
            @Nullable
            @Override
            public TableInfo apply(String input) {
                Matcher matcher = ReferenceInfos.SCHEMA_PATTERN.matcher(input);
                if (matcher.matches()) {
                    input = matcher.group(2);
                }
                return getTableInfo(input);
            }
        };
        tablesFilter = new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                //noinspection SimplifiableIfStatement
                if (BlobIndices.isBlobIndex(input)) {
                    return false;
                }
                Matcher matcher = ReferenceInfos.SCHEMA_PATTERN.matcher(input);
                return (matcher.matches() && matcher.group(1).equals(schemaName)) ;
            }
        };
    }

    private DocTableInfo innerGetTableInfo(String name) {
        boolean checkAliasSchema = clusterService.state().metaData().settings().getAsBoolean("crate.table_alias.schema_check", true);
        DocTableInfoBuilder builder = new DocTableInfoBuilder(
                this,
                new TableIdent(name(), name),
                clusterService,
                transportPutIndexTemplateAction,
                checkAliasSchema
        );
        return builder.build();
    }

    @Override
    public DocTableInfo getTableInfo(String name) {
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            throw new UnhandledServerException("Failed to get TableInfo", e.getCause());
        } catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause == null) {
                throw e;
            }
            if (cause instanceof TableUnknownException) {
                return null;
            }
            throw Throwables.propagate(cause);
        }
    }

    public Collection<String> tableNames() {
        // TODO: once we support closing/opening tables change this to concreteIndices()
        // and add  state info to the TableInfo.
        List<String> tables = new ArrayList<>();
        tables.addAll(Collections2.filter(
                Arrays.asList(clusterService.state().metaData().concreteAllOpenIndices()), tablesFilter));

        // Search for partitioned table templates
        UnmodifiableIterator<String> templates = clusterService.state().metaData().getTemplates().keysIt();
        while (templates.hasNext()) {
            String templateName = templates.next();
            try {
                String tableName = PartitionName.tableName(templateName);
                String schemaName = PartitionName.schemaName(templateName);
                if (schemaName.equalsIgnoreCase(name())) {
                    tables.add(tableName);
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
    public boolean systemSchema() {
        return false;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metaDataChanged() && cache.size() > 0) {
            cache.invalidateAll(event.indicesDeleted());

            // search for aliases of deleted and created indices, they must be invalidated also
            if (cache.size() > 0) {
                for (String index : event.indicesDeleted()) {
                    invalidateAliases(event.previousState().metaData().index(index).aliases());
                }
            }
            if (cache.size() > 0) {
                for (String index : event.indicesCreated()) {
                    invalidateAliases(event.state().metaData().index(index).aliases());
                }
            }

            // search for templates with changed meta data => invalidate template aliases
            if (cache.size() > 0
                    && !event.state().metaData().templates().equals(event.previousState().metaData().templates())) {
                // current state templates
                for (ObjectCursor<IndexTemplateMetaData> cursor : event.state().metaData().getTemplates().values()) {
                    invalidateAliases(cursor.value.aliases());
                }
                // previous state templates
                if (cache.size() > 0) {
                    for (ObjectCursor<IndexTemplateMetaData> cursor : event.previousState().metaData().getTemplates().values()) {
                        invalidateAliases(cursor.value.aliases());
                    }
                }
            }

            // search indices with changed meta data
            Iterator<String> it = cache.asMap().keySet().iterator();
            MetaData metaData = event.state().getMetaData();
            ObjectLookupContainer<String> templates = metaData.templates().keys();
            ImmutableOpenMap<String, IndexMetaData> indices = metaData.indices();
            while (it.hasNext()) {
                String tableName = it.next();

                IndexMetaData newIndexMetaData = event.state().getMetaData().index(tableName);
                if (newIndexMetaData != null && event.indexMetaDataChanged(newIndexMetaData)) {
                    cache.invalidate(tableName);
                    // invalidate aliases of changed indices
                    invalidateAliases(newIndexMetaData.aliases());

                    IndexMetaData oldIndexMetaData = event.previousState().metaData().index(tableName);
                    if (oldIndexMetaData != null) {
                        invalidateAliases(oldIndexMetaData.aliases());
                    }
                } else {
                    // this is the case if a single partition has been modified using alter table <t> partition (...)
                    String possibleTemplateName = PartitionName.templateName(name(), tableName);
                    if (templates.contains(possibleTemplateName)) {
                        for (ObjectObjectCursor<String, IndexMetaData> indexEntry : indices) {
                            if (PartitionName.isPartition(indexEntry.key)) {
                                cache.invalidate(tableName);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    private void invalidateAliases(ImmutableOpenMap<String, AliasMetaData> aliases) {
        assert aliases != null;
        if (aliases.size() > 0) {
            cache.invalidateAll(Arrays.asList(aliases.keys().toArray(String.class)));
        }
    }

    @Override
    public String toString() {
        return "DocSchemaInfo(" + name() + ")";
    }

    @Override
    public Iterator<TableInfo> iterator() {
        return Iterators.transform(tableNames().iterator(), tableInfoFunction);
    }
}