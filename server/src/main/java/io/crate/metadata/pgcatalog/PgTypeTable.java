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

import static io.crate.metadata.pgcatalog.OidHash.schemaOid;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.REGPROC;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.Regproc.REGPROC_ZERO;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.protocols.postgres.types.PGType;

public final class PgTypeTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_type");

    private static final Integer TYPE_NAMESPACE_OID = schemaOid(PgCatalogSchemaInfo.NAME);

    private PgTypeTable() {}

    public static SystemTable<PGType<?>> INSTANCE = SystemTable.<PGType<?>>builder(IDENT)
        .add("oid", INTEGER, PGType::oid)
        .add("typname", STRING, PGType::typName)
        .add("typnamespace", INTEGER, c -> TYPE_NAMESPACE_OID)
        .add("typowner", INTEGER, c -> null)
        .add("typlen", SHORT, PGType::typeLen)
        .add("typbyval", BOOLEAN, c -> true)
        .add("typtype", STRING, PGType::type)
        .add("typcategory", STRING, PGType::typeCategory)
        .add("typisdefined", BOOLEAN, c -> true)
        .add("typdelim", STRING, PGType::typDelim)
        // Zero for non-composite types, otherwise should point
        // to the pg_class table entry.
        .add("typrelid", INTEGER, c -> 0)
        .add("typsubscript", REGPROC, c -> REGPROC_ZERO)
        .add("typelem", INTEGER, PGType::typElem)
        .add("typarray", INTEGER, PGType::typArray)
        .add("typinput", REGPROC, PGType::typInput)
        .add("typoutput", REGPROC, PGType::typOutput)
        .add("typreceive", REGPROC, PGType::typReceive)
        .add("typsend", REGPROC, PGType::typSend)
        .add("typmodin", REGPROC, c -> REGPROC_ZERO)
        .add("typmodout", REGPROC, c -> REGPROC_ZERO)
        .add("typanalyze", REGPROC, c -> REGPROC_ZERO)
        // For typalign and typstorage follow Cockroach and return null:
        // https://github.com/cockroachdb/cockroach/blob/2e2e5c138e43c79d1d723e72326da05b6c5a1d84/pkg/sql/logictest/testdata/logic_test/pg_catalog#L2087
        .add("typalign", STRING, c -> null)
        .add("typstorage", STRING, c -> null)
        .add("typnotnull", BOOLEAN, c -> false)
        .add("typbasetype", INTEGER, c -> 0)
        .add("typtypmod", INTEGER, c -> -1)
        .add("typndims", INTEGER, c -> 0)
        .add("typcollation", INTEGER, c -> 0)
        .add("typdefaultbin", STRING, x -> null)
        .add("typdefault", STRING, c -> null)
        // should be `aclitem[]` but we lack `aclitem`, so going with same choice that Cockroach made:
        // https://github.com/cockroachdb/cockroach/blob/45deb66abbca3aae56bd27910a36d90a6a8bcafe/pkg/sql/vtable/pg_catalog.go#L809
        .add("typacl", STRING, c -> null)
        .build();
}
