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
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Iterator;

public class PgCatalogSchemaInfo implements SchemaInfo {

    public static final String NAME = "pg_catalog";
    private final ImmutableSortedMap<String, TableInfo> tableInfoMap;

    @Inject
    public PgCatalogSchemaInfo() {
        this.tableInfoMap = ImmutableSortedMap.<String, TableInfo>naturalOrder()
            .put(PgTypeTable.IDENT.name(), new PgTypeTable())
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
    public Iterator<TableInfo> iterator() {
        return tableInfoMap.values().iterator();
    }

    @Override
    public void update(ClusterChangedEvent event) {

    }
}
