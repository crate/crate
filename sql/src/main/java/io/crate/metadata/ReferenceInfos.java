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

import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReferenceInfos implements Iterable<SchemaInfo>, ClusterStateListener {

    public static final String DEFAULT_SCHEMA = DocSchemaInfo.NAME;
    public static final String SCHEMA_REGEX = "^([^.]+)\\..+";
    public static final Pattern SCHEMA_PATTERN = Pattern.compile(SCHEMA_REGEX);

    private final Map<String, SchemaInfo> builtInSchemas;
    private final SchemaInfo defaultSchemaInfo;
    private final ClusterService clusterService;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;

    private volatile Map<String, SchemaInfo> schemas = new HashMap<>();

    @Inject
    public ReferenceInfos(Map<String, SchemaInfo> builtInSchemas,
                          ClusterService clusterService,
                          TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        this.builtInSchemas = builtInSchemas;
        this.defaultSchemaInfo = builtInSchemas.get(DEFAULT_SCHEMA);
        this.clusterService = clusterService;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        schemas.putAll(builtInSchemas);
        schemas.putAll(resolveCustomSchemas(clusterService.state().metaData()));
        clusterService.add(this);
    }

    @Nullable
    public TableInfo getTableInfo(TableIdent ident) {
        SchemaInfo schemaInfo = getSchemaInfo(ident.schema());
        if (schemaInfo != null) {
            return schemaInfo.getTableInfo(ident.name());
        }
        return null;
    }

    /**
     *
     * @param ident the table ident to get a TableInfo for
     * @return an instance of TableInfo for the given ident, guaranteed to be not null
     * @throws io.crate.exceptions.SchemaUnknownException if schema given in <code>ident</code>
     *         does not exist
     * @throws io.crate.exceptions.TableUnknownException if table given in <code>ident</code> does
     *         not exist in the given schema
     */
    public TableInfo getTableInfoUnsafe(TableIdent ident) {
        TableInfo info;
        SchemaInfo schemaInfo = getSchemaInfo(ident.schema());
        if (schemaInfo == null) {
            throw new SchemaUnknownException(ident.schema());
        }
        try {
            info = schemaInfo.getTableInfo(ident.name());
            if (info == null) {
                throw new TableUnknownException(ident.name());
            }
        } catch (Exception e) {
            throw new TableUnknownException(ident.name(), e);
        }
        return info;
    }

    @Nullable
    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        TableInfo tableInfo = getTableInfo(ident.tableIdent());
        if (tableInfo != null) {
            return tableInfo.getReferenceInfo(ident.columnIdent());
        }
        return null;
    }

    @Nullable
    public SchemaInfo getSchemaInfo(@Nullable String schemaName) {
        if (schemaName == null) {
            return defaultSchemaInfo;
        } else {
            return schemas.get(schemaName);
        }
    }

    @Override
    public Iterator<SchemaInfo> iterator() {
        return schemas.values().iterator();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        Map<String, SchemaInfo> newSchemas = new HashMap<>();
        newSchemas.putAll(builtInSchemas);
        newSchemas.putAll(resolveCustomSchemas(event.state().metaData()));
        schemas = newSchemas;
    }

    /**
     * Create a custom schema info.
     *
     * @param name The schema name
     * @return an instance of SchemaInfo for the given name
     */
    private SchemaInfo getCustomSchemaInfo(final String name) {
        return new DocSchemaInfo(clusterService, transportPutIndexTemplateAction) {
            @Override
            public String name() {
                return name;
            }
        };
    }

    /**
     * Parse indices with custom schema name patterns out of the cluster state
     * and creates custom schema infos.
     *
     * @param metaData The cluster state meta data
     * @return a map of schema names and schema infos
     */
    private Map<String, SchemaInfo> resolveCustomSchemas(MetaData metaData) {
        Map<String, SchemaInfo> customSchemas = new HashMap<>();
        for (String index : metaData.concreteAllOpenIndices()) {
            Matcher matcher = ReferenceInfos.SCHEMA_PATTERN.matcher(index);
            if (matcher.matches()) {
                String schemaName = matcher.group(1);
                customSchemas.put(schemaName, getCustomSchemaInfo(schemaName));
            }
        }
        return customSchemas;
    }

}
