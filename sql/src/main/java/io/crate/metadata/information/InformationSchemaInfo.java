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

package io.crate.metadata.information;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collections;

@Singleton
public class InformationSchemaInfo implements SchemaInfo {

    public static final String NAME = "information_schema";

    private final ImmutableMap<String, TableInfo> tableInfoMap;

    @Inject
    public InformationSchemaInfo() {
        tableInfoMap = ImmutableSortedMap.<String, TableInfo>naturalOrder()
            .put(InformationTablesTableInfo.NAME, new InformationTablesTableInfo())
            .put(InformationViewsTableInfo.NAME, new InformationViewsTableInfo())
            .put(InformationColumnsTableInfo.NAME, InformationColumnsTableInfo.create())
            .put(InformationKeyColumnUsageTableInfo.NAME, InformationKeyColumnUsageTableInfo.create())
            .put(InformationPartitionsTableInfo.NAME, InformationPartitionsTableInfo.create())
            .put(InformationTableConstraintsTableInfo.NAME, new InformationTableConstraintsTableInfo())
            .put(InformationReferentialConstraintsTableInfo.NAME, new InformationReferentialConstraintsTableInfo())
            .put(InformationRoutinesTableInfo.NAME, new InformationRoutinesTableInfo())
            .put(InformationSchemataTableInfo.NAME, new InformationSchemataTableInfo())
            .put(InformationSqlFeaturesTableInfo.NAME, new InformationSqlFeaturesTableInfo())
            .build();
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
