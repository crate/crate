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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.UnhandledServerException;
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

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class DocSchemaInfo implements SchemaInfo, ClusterStateListener {

    public static final String NAME = Constants.DOC_SCHEMA_NAME;
    private final ClusterService clusterService;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;

    private final Predicate<String> tablesFilter = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
            if (BlobIndices.isBlobIndex(input)) {
                return false;
            }
            if (name().equalsIgnoreCase(DocSchemaInfo.NAME)) {
                return !input.matches(ReferenceInfos.SCHEMA_REGEX);
            } else {
                return input.matches(ReferenceInfos.SCHEMA_REGEX);
            }
        }
    };

    private final LoadingCache<String, DocTableInfo> cache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .build(
                    new CacheLoader<String, DocTableInfo>() {
                        @Override
                        public DocTableInfo load(String key) throws Exception {
                            return innerGetTableInfo(key);
                        }
                    }
            );
    private final Function<String, TableInfo> tableInfoFunction;

    @Inject
    public DocSchemaInfo(ClusterService clusterService,
                         TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        this.clusterService = clusterService;
        clusterService.add(this);
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        this.tableInfoFunction = new Function<String, TableInfo>() {
            @Nullable
            @Override
            public TableInfo apply(@Nullable String input) {
                if (input.matches(ReferenceInfos.SCHEMA_REGEX)) {
                    input = input.substring(name().length()+1);
                }
                return getTableInfo(input);
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
        // TODO: implement index based tables
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            throw new UnhandledServerException("Failed to get TableInfo", e.getCause());
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
        while(templates.hasNext()) {
            String templateName = templates.next();
            try {
                String tableName = PartitionName.tableName(templateName);
                tables.add(tableName);
            } catch (IllegalArgumentException e) {
                // do nothing
            }
        }

        return tables;
    }

    @Override
    public String name() {
        return NAME;
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
                    String possibleTemplateName = PartitionName.templateName(tableName);
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
    public Iterator<TableInfo> iterator() {
        return Iterators.transform(tableNames().iterator(), tableInfoFunction);
    }
}