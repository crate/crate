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
import com.google.common.base.Predicates;
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
import io.crate.metadata.Schemas;
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
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;

public class DocSchemaInfo implements SchemaInfo, ClusterStateListener {

    public static final String NAME = "doc";

    private final ClusterService clusterService;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;

    private final static Predicate<String> DOC_SCHEMA_TABLES_FILTER = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
            return !Schemas.SCHEMA_PATTERN.matcher(input).matches();
        }
    };

    private final Predicate<String> tablesFilter;

    private final LoadingCache<String, DocTableInfo> cache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .build(
                    new CacheLoader<String, DocTableInfo>() {
                        @Override
                        public DocTableInfo load(@Nonnull String key) throws Exception {
                            synchronized (DocSchemaInfo.this) {
                                return innerGetTableInfo(key);
                            }
                        }
                    }
            );

    private final Function<String, TableInfo> tableInfoFunction;
    private final String schemaName;
    private final ExecutorService executorService;
    private final Function<String, String> indexToTableName;
    private final static Function<String, String> AS_IS_FUNCTION = new Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String input) {
            return input;
        }
    };

    /**
     * DocSchemaInfo constructor for the default (doc) schema.
     */
    @Inject
    public DocSchemaInfo(ClusterService clusterService,
                         ThreadPool threadPool,
                         TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        this(Schemas.DEFAULT_SCHEMA_NAME,
                clusterService,
                (ExecutorService) threadPool.executor(ThreadPool.Names.SUGGEST),
                transportPutIndexTemplateAction,
                Predicates.and(Predicates.notNull(), DOC_SCHEMA_TABLES_FILTER),
                AS_IS_FUNCTION);
    }

    /**
     * constructor used for custom schemas
     */
    public DocSchemaInfo(final String schemaName,
                         ExecutorService executorService,
                         ClusterService clusterService,
                         TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        this(schemaName, clusterService, executorService, transportPutIndexTemplateAction,
                createSchemaNamePredicate(schemaName), new Function<String, String>() {
            @Nullable
            @Override
            public String apply(String input) {
                Matcher matcher = Schemas.SCHEMA_PATTERN.matcher(input);
                if (matcher.matches()) {
                    input = matcher.group(2);
                }
                return input;
            }
        });
    }

    private DocSchemaInfo(final String schemaName,
                          ClusterService clusterService,
                          ExecutorService executorService,
                          TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                          Predicate<String> tableFilter,
                          final Function<String, String> fqTableNameToTableName) {
        this.schemaName = schemaName;
        this.clusterService = clusterService;
        this.clusterService.add(this);
        this.executorService = executorService;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        this.tablesFilter = tableFilter;
        this.tableInfoFunction = new Function<String, TableInfo>() {
            @Nullable
            @Override
            public TableInfo apply(@Nullable String input) {
                assert input != null : "input must not be null";
                return getTableInfo(fqTableNameToTableName.apply(input));
            }
        };
        this.indexToTableName = new Function<String, String>() {
            @Nullable
            @Override
            public String apply(@Nullable String input) {
                if (input == null) {
                    return null;
                }
                if (BlobIndices.isBlobIndex(input)) {
                    return null;
                }
                if (PartitionName.isPartition(input)) {
                    return null;
                }
                return input;
            }
        };
    }

    private static Predicate<String> createSchemaNamePredicate(final String schemaName) {
        return Predicates.and(Predicates.notNull(), new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                Matcher matcher = Schemas.SCHEMA_PATTERN.matcher(input);
                return (matcher.matches() && matcher.group(1).equals(schemaName));
            }
        });
    }

    private DocTableInfo innerGetTableInfo(String name) {
        boolean checkAliasSchema = clusterService.state().metaData().settings().getAsBoolean("crate.table_alias.schema_check", true);
        DocTableInfoBuilder builder = new DocTableInfoBuilder(
                this,
                new TableIdent(name(), name),
                clusterService,
                transportPutIndexTemplateAction,
                executorService,
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

        Set<String> tables = new HashSet<>();
        tables.addAll(Collections2.filter(Collections2.transform(
                Arrays.asList(clusterService.state().metaData().concreteAllOpenIndices()), indexToTableName), tablesFilter));

        // Search for partitioned table templates
        UnmodifiableIterator<String> templates = clusterService.state().metaData().getTemplates().keysIt();
        while (templates.hasNext()) {
            String templateName = templates.next();
            if (!PartitionName.isPartition(templateName)) {
                continue;
            }
            try {
                PartitionName partitionName = PartitionName.fromIndexOrTemplate(templateName);
                String tableName = partitionName.tableName();
                String schemaName = partitionName.schema();
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
    public void invalidateTableCache(String tableName) {
        cache.invalidate(tableName);
    }

    @Override
    public synchronized void clusterChanged(ClusterChangedEvent event) {
        if (event.metaDataChanged()) {
            cache.invalidateAll(event.indicesDeleted());

            // search for aliases of deleted and created indices, they must be invalidated also
            for (String index : event.indicesDeleted()) {
                invalidateAliases(event.previousState().metaData().index(index).aliases());
            }
            for (String index : event.indicesCreated()) {
                invalidateAliases(event.state().metaData().index(index).aliases());
            }

            // search for templates with changed meta data => invalidate template aliases
            if (!event.state().metaData().templates().equals(event.previousState().metaData().templates())) {
                // current state templates
                for (ObjectCursor<IndexTemplateMetaData> cursor : event.state().metaData().getTemplates().values()) {
                    invalidateAliases(cursor.value.aliases());
                }
                // previous state templates
                for (ObjectCursor<IndexTemplateMetaData> cursor : event.previousState().metaData().getTemplates().values()) {
                    invalidateAliases(cursor.value.aliases());
                }
            }

            // search indices with changed meta data
            Iterator<String> it = cache.asMap().keySet().iterator();
            MetaData metaData = event.state().getMetaData();
            ObjectLookupContainer<String> templates = metaData.templates().keys();
            ImmutableOpenMap<String, IndexMetaData> indices = metaData.indices();
            while (it.hasNext()) {
                String tableName = it.next();
                String indexName = getIndexName(tableName);

                IndexMetaData newIndexMetaData = event.state().getMetaData().index(indexName);
                if (newIndexMetaData != null && event.indexMetaDataChanged(newIndexMetaData)) {
                    cache.invalidate(tableName);
                    // invalidate aliases of changed indices
                    invalidateAliases(newIndexMetaData.aliases());

                    IndexMetaData oldIndexMetaData = event.previousState().metaData().index(indexName);
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

    private String getIndexName(String tableName) {
        if (schemaName.equals(Schemas.DEFAULT_SCHEMA_NAME)) {
            return tableName;
        } else {
            return schemaName + "." + tableName;
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

    @Override
    public void close() throws Exception {
        clusterService.remove(this);
    }
}