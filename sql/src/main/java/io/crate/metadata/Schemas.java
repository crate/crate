/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.expression.udf.UserDefinedFunctionMetaData;
import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Singleton
public class Schemas extends AbstractLifecycleComponent implements Iterable<SchemaInfo>, ClusterStateListener {

    private static final Pattern SCHEMA_PATTERN = Pattern.compile("^([^.]+)\\.(.+)");

    /**
     * CrateDB's default schema name if the user hasn't specified anything.
     * Caution: Don't assume that this schema is _always_ set as the default!
     * {@see SessionContext}
     */
    public static final String DOC_SCHEMA_NAME = "doc";

    private final ClusterService clusterService;
    private final DocSchemaInfoFactory docSchemaInfoFactory;
    private final Map<String, SchemaInfo> schemas = new ConcurrentHashMap<>();
    private final Map<String, SchemaInfo> builtInSchemas;

    private final DefaultTemplateService defaultTemplateService;

    @Inject
    public Schemas(Settings settings,
                   Map<String, SchemaInfo> builtInSchemas,
                   ClusterService clusterService,
                   DocSchemaInfoFactory docSchemaInfoFactory) {
        super(settings);
        this.clusterService = clusterService;
        this.docSchemaInfoFactory = docSchemaInfoFactory;
        schemas.putAll(builtInSchemas);
        this.builtInSchemas = builtInSchemas;
        this.defaultTemplateService = new DefaultTemplateService(settings, clusterService);
    }

    /**
     * @param ident the table ident to get a TableInfo for
     * @return an instance of TableInfo for the given ident, guaranteed to be not null
     * @throws io.crate.exceptions.SchemaUnknownException if schema given in <code>ident</code>
     *                                                    does not exist
     * @throws RelationUnknown  if table given in <code>ident</code> does
     *                                                    not exist in the given schema
     */
    public <T extends TableInfo> T getTableInfo(TableIdent ident) {
        SchemaInfo schemaInfo = getSchemaInfo(ident);
        TableInfo info = schemaInfo.getTableInfo(ident.name());
        if (info == null) {
            throw new RelationUnknown(ident);
        }
        return (T) info;
    }

    /**
     * @param ident the table ident to get a TableInfo for
     * @param operation The opreation planned to be performed on the table
     * @return an instance of TableInfo for the given ident, guaranteed to be not null and to support the operation
     * required on it.
     * @throws io.crate.exceptions.SchemaUnknownException if schema given in <code>ident</code>
     *                                                    does not exist
     * @throws RelationUnknown  if table given in <code>ident</code> does
     *                                                    not exist in the given schema
     */
    public <T extends TableInfo> T getTableInfo(TableIdent ident, Operation operation) {
        TableInfo tableInfo = getTableInfo(ident);
        Operation.blockedRaiseException(tableInfo, operation);
        return (T) tableInfo;
    }

    private SchemaInfo getSchemaInfo(TableIdent ident) {
        String schemaName = ident.schema();
        SchemaInfo schemaInfo = schemas.get(schemaName);
        if (schemaInfo == null) {
            throw new SchemaUnknownException(schemaName);
        }
        return schemaInfo;
    }

    @Nonnull
    public Iterator<SchemaInfo> iterator() {
        return schemas.values().iterator();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        defaultTemplateService.createIfNotExists(event.state());

        Set<String> newCurrentSchemas = getNewCurrentSchemas(event.state().metaData());
        synchronized (schemas) {
            Sets.SetView<String> nonBuiltInSchemas = Sets.difference(schemas.keySet(), builtInSchemas.keySet());
            Set<String> deleted = Sets.difference(nonBuiltInSchemas, newCurrentSchemas).immutableCopy();
            Set<String> added = Sets.difference(newCurrentSchemas, schemas.keySet()).immutableCopy();

            for (String deletedSchema : deleted) {
                try {
                    schemas.remove(deletedSchema).close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }

            for (String addedSchema : added) {
                schemas.put(addedSchema, getCustomSchemaInfo(addedSchema));
            }

            // update all existing schemas
            for (SchemaInfo schemaInfo : this) {
                schemaInfo.update(event);
            }
        }
    }

    @VisibleForTesting
    static Set<String> getNewCurrentSchemas(MetaData metaData) {
        Set<String> schemas = new HashSet<>();
        // 'doc' schema is always available and has the special property that its indices
        // don't have to be prefixed with the schema name
        schemas.add(DOC_SCHEMA_NAME);
        for (String index : metaData.getConcreteAllIndices()) {
            addIfSchema(schemas, index);
        }
        for (ObjectCursor<String> cursor : metaData.templates().keys()) {
            addIfSchema(schemas, cursor.value);
        }
        UserDefinedFunctionsMetaData udfMetaData = metaData.custom(UserDefinedFunctionsMetaData.TYPE);
        if (udfMetaData != null) {
            udfMetaData.functionsMetaData()
                .stream()
                .map(UserDefinedFunctionMetaData::schema)
                .forEach(schemas::add);
        }
        return schemas;
    }

    private static void addIfSchema(Set<String> schemas, String indexOrTemplate) {
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
        return docSchemaInfoFactory.create(name, clusterService);
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

    public boolean tableExists(TableIdent tableIdent) {
        SchemaInfo schemaInfo = schemas.get(tableIdent.schema());
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

    @Override
    protected void doStart() {
        // add listener here to avoid guice proxy errors if the ClusterService could not be build
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }
}
