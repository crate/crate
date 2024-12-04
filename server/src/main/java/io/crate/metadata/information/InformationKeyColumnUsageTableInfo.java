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

import static io.crate.execution.engine.collect.sources.InformationSchemaIterables.PK_SUFFIX;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;

import io.crate.Constants;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

/**
 * Table which contains the primary keys of all user tables.
 */
public class InformationKeyColumnUsageTableInfo {

    public static final String NAME = "key_column_usage";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static SystemTable<InformationSchemaIterables.KeyColumnUsage> INSTANCE = SystemTable.<InformationSchemaIterables.KeyColumnUsage>builder(IDENT)
        .add("constraint_catalog", STRING, k -> Constants.DB_NAME)
        .add("constraint_schema", STRING, InformationSchemaIterables.KeyColumnUsage::getSchema)
        .add("constraint_name", STRING, k -> k.getTableName() + PK_SUFFIX)
        .add("table_catalog", STRING, k -> Constants.DB_NAME)
        .add("table_schema", STRING, InformationSchemaIterables.KeyColumnUsage::getSchema)
        .add("table_name", STRING, InformationSchemaIterables.KeyColumnUsage::getTableName)
        .add("column_name", STRING, InformationSchemaIterables.KeyColumnUsage::getPkColumnIdent)
        .add("ordinal_position", INTEGER, InformationSchemaIterables.KeyColumnUsage::getOrdinal)
        .setPrimaryKeys(
            ColumnIdent.of("constraint_catalog"),
            ColumnIdent.of("constraint_schema"),
            ColumnIdent.of("constraint_name"),
            ColumnIdent.of("column_name")
        )
        .build();
}
