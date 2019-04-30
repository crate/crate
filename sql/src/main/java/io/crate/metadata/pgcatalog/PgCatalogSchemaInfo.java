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

package io.crate.metadata.pgcatalog;

import com.google.common.collect.ImmutableSortedMap;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Collections;

public class PgCatalogSchemaInfo implements SchemaInfo {

    public static final String NAME = "pg_catalog";
    private final ImmutableSortedMap<String, TableInfo> tableInfoMap;
    private final UserDefinedFunctionService udfService;

    @Inject
    public PgCatalogSchemaInfo(UserDefinedFunctionService udfService) {
        this.udfService = udfService;
        tableInfoMap = ImmutableSortedMap.<String, TableInfo>naturalOrder()
            .put(PgTypeTable.IDENT.name(), new PgTypeTable())
            .put(PgClassTable.IDENT.name(), new PgClassTable())
            .put(PgNamespaceTable.IDENT.name(), new PgNamespaceTable())
            .put(PgAttrDefTable.IDENT.name(), new PgAttrDefTable())
            .put(PgAttributeTable.IDENT.name(), new PgAttributeTable())
            .put(PgIndexTable.IDENT.name(), new PgIndexTable())
            .put(PgConstraintTable.IDENT.name(), new PgConstraintTable())
            .put(PgDatabaseTable.NAME.name(), new PgDatabaseTable())
            .put(PgDescriptionTable.NAME.name(), new PgDescriptionTable())
            .put(PgSettingsTable.IDENT.name(), new PgSettingsTable())
            .build();
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
        assert event.metaDataChanged() : "metaDataChanged must be true if update is called";
        MetaData newMetaData = event.state().metaData();
        // re register UDFs for this schema
        UserDefinedFunctionsMetaData udfMetaData = newMetaData.custom(UserDefinedFunctionsMetaData.TYPE);
        if (udfMetaData != null) {
            udfService.updateImplementations(
                NAME,
                udfMetaData.functionsMetaData().stream().filter(f -> NAME.equals(f.schema())));
        }
    }
}
