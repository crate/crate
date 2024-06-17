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
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

import io.crate.Constants;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public final class PgDatabaseTable {

    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_database");

    private PgDatabaseTable() {}

    public static SystemTable<Void> INSTANCE = SystemTable.<Void>builder(NAME)
        .add("oid", INTEGER, c -> Constants.DB_OID)
        .add("datname", STRING, c -> Constants.DB_NAME)
        .add("datdba", INTEGER, c -> 1)
        .add("encoding", INTEGER, c -> 6)
        .add("datcollate", STRING, c -> "en_US.UTF-8")
        .add("datctype", STRING, c -> "en_US.UTF-8")
        .add("datistemplate", BOOLEAN,c -> false)
        .add("datallowconn", BOOLEAN, c -> true)
        .add("datconnlimit", INTEGER,c -> -1) // no limit
        // We don't have any good values for these
        .add("datlastsysoid", INTEGER, c -> null)
        .add("datfrozenxid", INTEGER, c -> null)
        .add("datminmxid", INTEGER, c -> null)
        .add("dattablespace", INTEGER, c -> null)
        // should be `aclitem[]` but we lack `aclitem`, so going with same choice that Cockroach made:
        // https://github.com/cockroachdb/cockroach/blob/45deb66abbca3aae56bd27910a36d90a6a8bcafe/pkg/sql/vtable/pg_catalog.go#L277
        .add("datacl", STRING_ARRAY, c -> null)
        .build();
}
