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

package io.crate.metadata.pgcatalog;


import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.SystemTable;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.statistics.TableStats;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

@Singleton
public class PgCatalogSchemaInfo implements SchemaInfo {

    public static final String NAME = "pg_catalog";
    private final Map<String, TableInfo> tableInfoMap;
    private final UserDefinedFunctionService udfService;
    private final SystemTable<PgClassTable.Entry> pgClassTable;

    @Inject
    public PgCatalogSchemaInfo(UserDefinedFunctionService udfService, TableStats tableStats) {
        this.udfService = udfService;
        this.pgClassTable = PgClassTable.create(tableStats);
        tableInfoMap = Map.<String, TableInfo>ofEntries(
            Map.entry(PgStatsTable.NAME.name(), PgStatsTable.create()),
            Map.entry(PgTypeTable.IDENT.name(), PgTypeTable.create()),
            Map.entry(PgClassTable.IDENT.name(), pgClassTable),
            Map.entry(PgNamespaceTable.IDENT.name(), PgNamespaceTable.create()),
            Map.entry(PgAttrDefTable.IDENT.name(), PgAttrDefTable.create()),
            Map.entry(PgAttributeTable.IDENT.name(), PgAttributeTable.create()),
            Map.entry(PgIndexTable.IDENT.name(), PgIndexTable.create()),
            Map.entry(PgConstraintTable.IDENT.name(), PgConstraintTable.create()),
            Map.entry(PgDatabaseTable.NAME.name(), PgDatabaseTable.create()),
            Map.entry(PgDescriptionTable.NAME.name(), PgDescriptionTable.create()),
            Map.entry(PgSettingsTable.IDENT.name(), PgSettingsTable.create()),
            Map.entry(PgProcTable.IDENT.name(), PgProcTable.create()),
            Map.entry(PgRangeTable.IDENT.name(), PgRangeTable.create()),
            Map.entry(PgEnumTable.IDENT.name(), PgEnumTable.create()),
            Map.entry(PgRolesTable.IDENT.name(), PgRolesTable.create()),
            Map.entry(PgAmTable.IDENT.name(), PgAmTable.create()),
            Map.entry(PgTablespaceTable.IDENT.name(), PgTablespaceTable.create()),
            Map.entry(PgIndexesTable.IDENT.name(), PgIndexesTable.create()),
            Map.entry(PgPublicationTable.IDENT.name(), PgPublicationTable.create())
        );
    }

    SystemTable<PgClassTable.Entry> pgClassTable() {
        return pgClassTable;
    }

    @Nullable
    @Override
    public TableInfo getTableInfo(String name) {
        return tableInfoMap.get(name);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void invalidateTableCache(String tableName) {
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public Iterable<TableInfo> getTables() {
        return tableInfoMap.values();
    }


    @Override
    public Iterable<ViewInfo> getViews() {
        return Collections.emptyList();
    }

    @Override
    public void update(ClusterChangedEvent event) {
        assert event.metadataChanged() : "metadataChanged must be true if update is called";
        Metadata newMetadata = event.state().metadata();
        // re register UDFs for this schema
        UserDefinedFunctionsMetadata udfMetadata = newMetadata.custom(UserDefinedFunctionsMetadata.TYPE);
        if (udfMetadata != null) {
            udfService.updateImplementations(
                NAME,
                udfMetadata.functionsMetadata().stream().filter(f -> NAME.equals(f.schema())));
        }
    }
}
