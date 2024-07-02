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

import static io.crate.metadata.pgcatalog.OidHash.constraintOid;
import static io.crate.metadata.pgcatalog.OidHash.relationOid;
import static io.crate.metadata.pgcatalog.OidHash.schemaOid;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.INTEGER_ARRAY;
import static io.crate.types.DataTypes.SHORT_ARRAY;
import static io.crate.types.DataTypes.STRING;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.table.ConstraintInfo;

public final class PgConstraintTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_constraint");

    private static final String NO_ACTION = "a";
    private static final String MATCH_SIMPLE = "s";

    private PgConstraintTable() {}

    public static SystemTable<ConstraintInfo> INSTANCE = SystemTable.<ConstraintInfo>builder(IDENT)
        .add("oid", INTEGER, c -> constraintOid(c.relationName().fqn(), c.constraintName(), c.constraintType().toString()))
        .add("conname", STRING, ConstraintInfo::constraintName)
        .add("connamespace", INTEGER, c -> schemaOid(c.relationName().schema()))
        .add("contype", STRING, c -> c.constraintType().postgresChar())
        .add("condeferrable", BOOLEAN, c -> false)
        .add("condeferred", BOOLEAN, c -> false)
        .add("convalidated", BOOLEAN, c -> true)
        .add("conrelid", INTEGER, c -> relationOid(c.relationInfo()))
        .add("contypid", INTEGER, c -> 0)
        .add("conindid", INTEGER, c -> 0)
        .add("conparentid", INTEGER, c -> 0)
        .add("confrelid", INTEGER, c -> 0)
        .add("confupdtype", STRING, c -> NO_ACTION)
        .add("confdeltype", STRING, c -> NO_ACTION)
        .add("confmatchtype", STRING, c -> MATCH_SIMPLE)
        .add("conislocal", BOOLEAN, c -> true)
        .add("coninhcount", INTEGER, c -> 0)
        .add("connoinherit", BOOLEAN, c -> true)
        .add("conkey", SHORT_ARRAY, c -> c.conkey())
        .add("confkey", SHORT_ARRAY, c -> null)
        .add("conpfeqop", INTEGER_ARRAY, c -> null)
        .add("conppeqop", INTEGER_ARRAY, c -> null)
        .add("conffeqop", INTEGER_ARRAY, c -> null)
        .add("conexclop", INTEGER_ARRAY, c -> null)
        .add("conbin", STRING, c -> null)
        .build();
}
