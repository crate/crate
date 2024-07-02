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

public final class PgLocksTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_locks");

    private PgLocksTable() {}

    public static SystemTable<Void> INSTANCE = SystemTable.<Void>builder(IDENT)
        .add("locktype", DataTypes.STRING, x -> null)
        .add("database", DataTypes.INTEGER, x -> null)
        .add("relation", DataTypes.INTEGER, x -> null)
        .add("page", DataTypes.INTEGER, x -> null)
        .add("tuple", DataTypes.SHORT, x -> null)
        .add("virtualxid", DataTypes.STRING, x -> null)
        .add("transactionid", DataTypes.INTEGER, x -> null)
        .add("classid", DataTypes.INTEGER, x -> null)
        .add("objid", DataTypes.STRING, x -> null)
        .add("objsubid", DataTypes.SHORT, x -> null)
        .add("virtualtransaction", DataTypes.STRING, x -> null)
        .add("pid", DataTypes.INTEGER, x -> null)
        .add("mode", DataTypes.STRING, x -> null)
        .add("granted", DataTypes.BOOLEAN, x -> null)
        .add("fastpath", DataTypes.BOOLEAN, x -> null)
        .add("waitstart", DataTypes.TIMESTAMPZ, x -> null)
        .build();
}
