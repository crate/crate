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

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.STRING;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.table.TableInfo;

public final class PgTablesTable {

    public static final String NAME = "pg_tables";
    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, NAME);

    private PgTablesTable() {}

    public static SystemTable<TableInfo> INSTANCE = SystemTable.<TableInfo>builder(IDENT)
        .add("schemaname", STRING, r -> r.ident().schema())
        .add("tablename", STRING, r -> r.ident().name())
        .add("tableowner", STRING, r -> null)
        .add("tablespace", STRING, r -> null)
        .add("hasindexes", BOOLEAN, r -> false)
        .add("hasrules", BOOLEAN, r -> false)
        .add("hastriggers", BOOLEAN, r -> false)
        .add("rowsecurity", BOOLEAN, r -> false)
        .build();
}
