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

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.types.DataTypes;

public final class PgTablespaceTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_tablespace");

    public static SystemTable<Void> create() {
        return SystemTable.<Void>builder(IDENT)
            .add("oid", DataTypes.INTEGER, ignored -> null)
            .add("spcname", DataTypes.STRING, ignored -> null)
            .add("spcowner", DataTypes.INTEGER, ignored -> null)
            // should be `aclitem[]` but we lack `aclitem`, so going with same choice that Cockroach made:
            // https://github.com/cockroachdb/cockroach/blob/45deb66abbca3aae56bd27910a36d90a6a8bcafe/pkg/sql/vtable/pg_catalog.go#L746
            .add("spcacl", DataTypes.STRING_ARRAY, ignored -> null)
            .add("spcoptions", DataTypes.STRING_ARRAY, ignored -> null)
            .build();
    }
}
