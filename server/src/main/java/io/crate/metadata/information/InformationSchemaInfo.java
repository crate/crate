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

package io.crate.metadata.information;

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.common.collections.MapBuilder;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;

@Singleton
public class InformationSchemaInfo implements SchemaInfo {

    public static final String NAME = "information_schema";

    private final Map<String, TableInfo> tableInfoMap;

    @Inject
    public InformationSchemaInfo() {
        tableInfoMap = MapBuilder.<String, TableInfo>treeMapBuilder()
            .put(InformationTablesTableInfo.NAME, InformationTablesTableInfo.create())
            .put(InformationViewsTableInfo.NAME, InformationViewsTableInfo.create())
            .put(InformationColumnsTableInfo.NAME, InformationColumnsTableInfo.create())
            .put(InformationKeyColumnUsageTableInfo.NAME, InformationKeyColumnUsageTableInfo.create())
            .put(InformationPartitionsTableInfo.NAME, InformationPartitionsTableInfo.create())
            .put(InformationTableConstraintsTableInfo.NAME, InformationTableConstraintsTableInfo.create())
            .put(InformationReferentialConstraintsTableInfo.NAME, InformationReferentialConstraintsTableInfo.create())
            .put(InformationRoutinesTableInfo.NAME, InformationRoutinesTableInfo.create())
            .put(InformationSchemataTableInfo.NAME, InformationSchemataTableInfo.create())
            .put(InformationSqlFeaturesTableInfo.NAME, InformationSqlFeaturesTableInfo.create())
            .put(InformationCharacterSetsTable.NAME, InformationCharacterSetsTable.create())
            .put(ForeignServerTableInfo.NAME, ForeignServerTableInfo.create())
            .put(ForeignTableTableInfo.NAME, ForeignTableTableInfo.create())
            .put(UserMappingsTableInfo.NAME, UserMappingsTableInfo.create())
            .immutableMap();
    }

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
    public Iterable<TableInfo> getTables() {
        return tableInfoMap.values();
    }

    @Override
    public Iterable<ViewInfo> getViews() {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void update(ClusterChangedEvent event) {

    }
}
