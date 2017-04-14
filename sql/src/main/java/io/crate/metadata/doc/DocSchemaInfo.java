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
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.blob.v2.BlobIndex;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.*;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.udf.UDFLanguage;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.operation.udf.UserDefinedFunctionsMetaData;
import io.crate.types.DataType;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
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
    private static final Logger LOGGER = Loggers.getLogger(DocSchemaInfo.class);

    private final ClusterService clusterService;
    private final DocTableInfoFactory docTableInfoFactory;
    private final Functions functions;
    private final UserDefinedFunctionService udfService;

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

    private static final Predicate<String> NO_BLOB = ((Predicate<String>)BlobIndex::isBlobIndex).negate();
    private static final Predicate<String> NO_PARTITION = ((Predicate<String>)PartitionName::isPartition).negate();

    private final String schemaName;
    private boolean isDocSchema;

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
        this.isDocSchema = Schemas.DEFAULT_SCHEMA_NAME.equals(schemaName);
        this.clusterService = clusterService;
        this.udfService = udfService;
        this.docTableInfoFactory = docTableInfoFactory;
    }

    private static String getTableNameFromIndexName(String indexName) {
        Matcher matcher = Schemas.SCHEMA_PATTERN.matcher(indexName);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        return indexName;
    }

    private boolean indexMatchesSchema(String index) {
        Matcher matcher = Schemas.SCHEMA_PATTERN.matcher(index);
        if (matcher.matches()) {
            return matcher.group(1).equals(schemaName);
        }
        return isDocSchema;
    }

    private DocTableInfo innerGetTableInfo(String tableName) {
        return docTableInfoFactory.create(new TableIdent(schemaName, tableName), clusterService);
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
            if (cause instanceof ResourceUnknownException) {
                return null;
            }
            throw Throwables.propagate(cause);
        }
    }

    private Collection<String> tableNames() {
        Set<String> tables = new HashSet<>();

        Stream.of(clusterService.state().metaData().getConcreteAllOpenIndices())
            .filter(NO_BLOB)
            .filter(NO_PARTITION)
            .filter(this::indexMatchesSchema)
            .map(DocSchemaInfo::getTableNameFromIndexName)
            .forEach(tables::add);

        // Search for partitioned table templates
        Iterator<String> templates = clusterService.state().metaData().getTemplates().keysIt();
        while (templates.hasNext()) {
            String templateName = templates.next();
            if (!PartitionName.isPartition(templateName)) {
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
        cache.invalidate(tableName);
    }

    @Override
    public void update(ClusterChangedEvent event) {
        assert event.metaDataChanged() : "metaDataChanged must be true if update is called";

        // search for aliases of deleted and created indices, they must be invalidated also
        MetaData prevMetaData = event.previousState().metaData();
        for (Index index : event.indicesDeleted()) {
            invalidateAliases(prevMetaData.index(index).getAliases());
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
        Iterator<String> currentTablesIt = cache.asMap().keySet().iterator();
        ObjectLookupContainer<String> templates = newTemplates.keys();
        ImmutableOpenMap<String, IndexMetaData> indices = newMetaData.indices();
        while (currentTablesIt.hasNext()) {
            String tableName = currentTablesIt.next();
            String indexName = getIndexName(tableName);

            IndexMetaData newIndexMetaData = newMetaData.index(indexName);
            if (newIndexMetaData == null) {
                cache.invalidate(tableName);
            } else {
                IndexMetaData oldIndexMetaData = prevMetaData.index(indexName);
                if (oldIndexMetaData != null && ClusterChangedEvent.indexMetaDataChanged(oldIndexMetaData, newIndexMetaData)) {
                    cache.invalidate(tableName);
                    // invalidate aliases of changed indices
                    invalidateAliases(newIndexMetaData.getAliases());
                    invalidateAliases(oldIndexMetaData.getAliases());
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

        // re register UDFs for this schema
        functions.deregisterSchemaFunctions(schemaName);
        UserDefinedFunctionsMetaData udfMetaData = newMetaData.custom(UserDefinedFunctionsMetaData.TYPE);
        if (udfMetaData != null) {
            Stream<UserDefinedFunctionMetaData> udfFunctions = udfMetaData.functionsMetaData().stream()
                .filter(function -> schemaName.equals(function.schema()));
            // TODO: the functions field should actually be moved to the udfService for better encapsulation
            // then the registration of the function implementations of a schema would also move to the udfService
            functions.registerSchemaFunctionResolvers(schemaName, toFunctionImpl(udfFunctions::iterator));
        }
    }

    @VisibleForTesting
    Map<FunctionIdent, FunctionImplementation> toFunctionImpl(Iterable<UserDefinedFunctionMetaData> functionsMetadata) {
        Map<FunctionIdent, FunctionImplementation> udfFunctions = new HashMap<>();
        for (UserDefinedFunctionMetaData function : functionsMetadata) {
            try {
                UDFLanguage lang = udfService.getLanguage(function.language());
                FunctionImplementation impl = lang.createFunctionImplementation(function);
                udfFunctions.put(impl.info().ident(), impl);
            } catch (javax.script.ScriptException | IllegalArgumentException e) {
                LOGGER.warn(
                    String.format(Locale.ENGLISH, "Can't create user defined function '%s(%s)'",
                        function.name(),
                        function.argumentTypes().stream().map(DataType::getName).collect(Collectors.joining(", "))
                    ), e);
            }
        }
        return udfFunctions;
    }

    private String getIndexName(String tableName) {
        if (schemaName.equals(Schemas.DEFAULT_SCHEMA_NAME)) {
            return tableName;
        } else {
            return schemaName + "." + tableName;
        }
    }

    private void invalidateAliases(ImmutableOpenMap<String, AliasMetaData> aliases) {
        assert aliases != null : "aliases must not be null";
        if (aliases.size() > 0) {
            aliases.keysIt().forEachRemaining(cache::invalidate);
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
        functions.deregisterSchemaFunctions(schemaName);
    }
}
