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

package io.crate.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Sets;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;

import static com.google.common.base.MoreObjects.firstNonNull;

@Singleton
public class ReferenceInfos implements ClusterStateListener, Schemas {

    private final static ESLogger LOGGER = Loggers.getLogger(ReferenceInfos.class);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;
    private final ExecutorService executorService;
    private final Functions functions;

    private final Map<String, SchemaInfo> schemas = new ConcurrentHashMap<>();
    private final Map<String, SchemaInfo> builtInSchemas;

    @Inject
    public ReferenceInfos(Map<String, SchemaInfo> builtInSchemas,
                          ClusterService clusterService,
                          IndexNameExpressionResolver indexNameExpressionResolver,
                          ThreadPool threadPool,
                          TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                          Functions functions) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        this.functions = functions;
        this.executorService = (ExecutorService) threadPool.executor(ThreadPool.Names.SUGGEST);
        schemas.putAll(builtInSchemas);
        this.builtInSchemas = builtInSchemas;
        clusterService.add(this);
    }

    @Override
    public DocTableInfo getDropableTable(TableIdent tableIdent) {
        TableInfo tableInfo = getTableInfo(tableIdent);
        if (!(tableInfo instanceof DocTableInfo)) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "The table %s is not dropable.", tableInfo.ident()));
        }
        DocTableInfo docTableInfo = (DocTableInfo) tableInfo;
        if (docTableInfo.isAlias() && !docTableInfo.isPartitioned() && !isOrphanedAlias(docTableInfo)) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "%s is an alias and hence not dropable.", tableInfo.ident()));
        }
        return docTableInfo;
    }

    @Override
    public DocTableInfo getWritableTable(TableIdent tableIdent) {
        TableInfo tableInfo = getTableInfo(tableIdent);
        if (!(tableInfo instanceof DocTableInfo)) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "The table %s is read-only. Write, Drop or Alter operations are not supported", tableInfo.ident()));
        }
        DocTableInfo docTableInfo = (DocTableInfo) tableInfo;
        if (docTableInfo.isAlias() && !docTableInfo.isPartitioned()) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "%s is an alias. Write, Drop or Alter operations are not supported", tableInfo.ident()));
        }
        return docTableInfo;
    }

    /**
     * @param ident the table ident to get a TableInfo for
     * @return an instance of TableInfo for the given ident, guaranteed to be not null
     * @throws io.crate.exceptions.SchemaUnknownException if schema given in <code>ident</code>
     *                                                    does not exist
     * @throws io.crate.exceptions.TableUnknownException  if table given in <code>ident</code> does
     *                                                    not exist in the given schema
     */
    @Override
    public TableInfo getTableInfo(TableIdent ident) {
        SchemaInfo schemaInfo = getSchemaInfo(ident);
        TableInfo info;
        info = schemaInfo.getTableInfo(ident.name());
        if (info == null) {
            throw new TableUnknownException(ident);
        }
        return info;
    }

    private SchemaInfo getSchemaInfo(TableIdent ident) {
        String schemaName = firstNonNull(ident.schema(), DEFAULT_SCHEMA_NAME);
        SchemaInfo schemaInfo = schemas.get(schemaName);
        if (schemaInfo == null) {
            throw new SchemaUnknownException(schemaName);
        }
        return schemaInfo;
    }

    @Override
    @Nonnull
    public Iterator<SchemaInfo> iterator() {
        return schemas.values().iterator();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }

        Set<String> newCurrentSchemas = getNewCurrentSchemas(event.state().metaData());
        synchronized (schemas) {
            Sets.SetView<String> nonBuiltInSchemas = Sets.difference(schemas.keySet(), builtInSchemas.keySet());
            Set<String> deleted = Sets.difference(nonBuiltInSchemas, newCurrentSchemas).immutableCopy();
            Set<String> added = Sets.difference(newCurrentSchemas, schemas.keySet()).immutableCopy();

            for (String deletedSchema : deleted) {
                try {
                    schemas.remove(deletedSchema).close();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            for (String addedSchema : added) {
                schemas.put(addedSchema, getCustomSchemaInfo(addedSchema));
            }
        }
    }

    private Set<String> getNewCurrentSchemas(MetaData metaData) {
        Set<String> schemas = new HashSet<>();
        for (String openIndex : metaData.concreteAllOpenIndices()) {
            addIfSchema(schemas, openIndex);
        }
        for (ObjectCursor<String> cursor : metaData.templates().keys()) {
            addIfSchema(schemas, cursor.value);
        }
        return schemas;
    }

    private void addIfSchema(Set<String> schemas, String indexOrTemplate) {
        Matcher matcher = SCHEMA_PATTERN.matcher(indexOrTemplate);
        if (matcher.matches()) {
            schemas.add(matcher.group(1));
        }
    }

    /**
     * Create a custom schema info.
     *
     * @param name The schema name
     * @return an instance of SchemaInfo for the given name
     */
    private SchemaInfo getCustomSchemaInfo(String name) {
        return new DocSchemaInfo(name, executorService, clusterService, indexNameExpressionResolver, transportPutIndexTemplateAction, functions);
    }

    /**
     * Checks if a given schema name string is a user defined schema or the default one.
     *
     * @param schemaName The schema name as a string.
     */
    public static boolean isDefaultOrCustomSchema(@Nullable String schemaName) {
        if (schemaName == null) {
            return true;
        }
        if (schemaName.equalsIgnoreCase(InformationSchemaInfo.NAME)
                || schemaName.equalsIgnoreCase(SysSchemaInfo.NAME)
                || schemaName.equalsIgnoreCase(BlobSchemaInfo.NAME)
                ) {
            return false;
        }
        return true;
    }

    @Override
    public boolean tableExists(TableIdent tableIdent) {
        SchemaInfo schemaInfo = schemas.get(firstNonNull(tableIdent.schema(), DEFAULT_SCHEMA_NAME));
        if (schemaInfo == null) {
            return false;
        }
        schemaInfo.invalidateTableCache(tableIdent.name());
        TableInfo tableInfo = schemaInfo.getTableInfo(tableIdent.name());
        if (tableInfo == null) {
            return false;
        } else if ((tableInfo instanceof DocTableInfo)) {
            return !isOrphanedAlias((DocTableInfo) tableInfo);
        }
        return true;
    }

    /**
     * checks if the given TableInfo has been created from an orphaned alias left from
     * an incomplete drop table on a partitioned table
     */
    public boolean isOrphanedAlias(DocTableInfo table) {
        if (table.isPartitioned() && table.isAlias()
                && table.concreteIndices().length >= 1) {
            String templateName = PartitionName.templateName(table.ident().schema(), table.ident().name());
            MetaData metaData = clusterService.state().metaData();
            if (!metaData.templates().containsKey(templateName)) {
                // template or alias missing
                return true;
            }
        }
        return false;
    }
}
