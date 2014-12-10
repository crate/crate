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
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.inject.Singleton;

import java.util.Iterator;

@Singleton
public class InformationSchemaInfo implements SchemaInfo {

    public static final String NAME = "information_schema";

    public final ImmutableMap<String, InformationTableInfo> tableInfoMap;

    public InformationSchemaInfo() {
        this.tableInfoMap = ImmutableMap.<String, InformationTableInfo>builder()
                .put(InformationTablesTableInfo.NAME, new InformationTablesTableInfo(this))
                .put(InformationColumnsTableInfo.NAME, new InformationColumnsTableInfo(this))
                .put(InformationPartitionsTableInfo.NAME, new InformationPartitionsTableInfo(this))
                .put(InformationTableConstraintsTableInfo.NAME, new InformationTableConstraintsTableInfo(this))
                .put(InformationRoutinesTableInfo.NAME, new InformationRoutinesTableInfo(this))
                .put(InformationSchemataTableInfo.NAME, new InformationSchemataTableInfo(this))
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
    public boolean systemSchema() {
        return true;
    }

    @Override
    public Iterator<? extends TableInfo> iterator() {
        return tableInfoMap.values().iterator();
    }
}
