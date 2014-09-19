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
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import java.util.Collection;
import java.util.Iterator;

public class InformationSchemaInfo implements SchemaInfo {

    public static final String NAME = "information_schema";

    public static final InformationTableInfo TABLE_INFO_TABLES = new InformationTableInfo.Builder("tables")
            .add("schema_name", DataTypes.STRING, null)
            .add("table_name", DataTypes.STRING, null)
            .add("number_of_shards", DataTypes.INTEGER, null)
            .add("number_of_replicas", DataTypes.STRING, null)
            .add("clustered_by", DataTypes.STRING, null)
            .add("partitioned_by", new ArrayType(DataTypes.STRING), null)
            .add("blobs_path", DataTypes.STRING, null)
            .addPrimaryKey("schema_name")
            .addPrimaryKey("table_name")
            .build();

    public static final InformationTableInfo TABLE_INFO_TABLE_PARTITIONS = new InformationTableInfo.Builder("table_partitions")
            .add("table_name", DataTypes.STRING, null)
            .add("schema_name", DataTypes.STRING, null)
            .add("partition_ident", DataTypes.STRING, null)
            .add("values", DataTypes.OBJECT, null)
            .build();

    public static final InformationTableInfo TABLE_INFO_COLUMNS = new InformationTableInfo.Builder("columns")
            .add("schema_name", DataTypes.STRING, null)
            .add("table_name", DataTypes.STRING, null)
            .add("column_name", DataTypes.STRING, null)
            .add("ordinal_position", DataTypes.SHORT, null)
            .add("data_type", DataTypes.STRING, null)
            .addPrimaryKey("schema_name")
            .addPrimaryKey("table_name")
            .addPrimaryKey("column_name")
            .build();

    public static final InformationTableInfo TABLE_INFO_TABLE_CONSTRAINTS = new InformationTableInfo.Builder("table_constraints")
            .add("schema_name", DataTypes.STRING, null)
            .add("table_name", DataTypes.STRING, null)
            .add("constraint_name", new ArrayType(DataTypes.STRING), null)
            .add("constraint_type", DataTypes.STRING, null)
            .build();

    public static final InformationTableInfo TABLE_INFO_ROUTINES = new InformationTableInfo.Builder("routines")
            .add("routine_name", DataTypes.STRING, null)
            .add("routine_type", DataTypes.STRING, null)
            .build();

    public static final ImmutableMap<String, TableInfo> TABLE_INFOS =
            ImmutableMap.<String, TableInfo>builder()
                    .put(TABLE_INFO_TABLES.ident().name(), TABLE_INFO_TABLES)
                    .put(TABLE_INFO_TABLE_PARTITIONS.ident().name(), TABLE_INFO_TABLE_PARTITIONS)
                    .put(TABLE_INFO_COLUMNS.ident().name(), TABLE_INFO_COLUMNS)
                    .put(TABLE_INFO_TABLE_CONSTRAINTS.ident().name(), TABLE_INFO_TABLE_CONSTRAINTS)
                    .put(TABLE_INFO_ROUTINES.ident().name(), TABLE_INFO_ROUTINES)
                    .build();

    @Override
    public TableInfo getTableInfo(String name) {
        return TABLE_INFOS.get(name);
    }

    @Override
    public Collection<String> tableNames() {
        return TABLE_INFOS.keySet();
    }

    @Override
    public boolean systemSchema() {
        return true;
    }

    @Override
    public Iterator<TableInfo> iterator() {
        return TABLE_INFOS.values().iterator();
    }
}
