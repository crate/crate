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

import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.TIMESTAMPZ;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public final class PgRolesTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_roles");

    public static SystemTable<Void> create() {
        return SystemTable.<Void>builder(IDENT)
            .add("oid", INTEGER, ignored -> null)
            .add("rolname", STRING, ignored -> null)
            .add("rolsuper", BOOLEAN, ignored -> null)
            .add("rolinherit", BOOLEAN, ignored -> null)
            .add("rolcreaterole", BOOLEAN, ignored -> null)
            .add("rolcreatedb", BOOLEAN, ignored -> null)
            .add("rolcanlogin", BOOLEAN, ignored -> null)
            .add("rolreplication", BOOLEAN, ignored -> null)
            .add("rolconnlimit", INTEGER, ignored -> null)
            .add("rolpassword", STRING, ignored -> null)
            .add("rolvaliduntil", TIMESTAMPZ, ignored -> null)
            .add("rolbypassrls", BOOLEAN, ignored -> null)
            .add("rolconfig", STRING_ARRAY, ignored -> null)
            .build();
    }
}
