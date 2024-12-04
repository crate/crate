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

import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.REGPROC;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public final class PgRangeTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_range");

    private PgRangeTable() {}

    public static SystemTable<Void> INSTANCE = SystemTable.<Void>builder(IDENT)
        .add("rngtypid", INTEGER, ignored -> null)
        .add("rngsubtype", INTEGER, ignored -> null)
        .add("rngmultitypid", INTEGER, ignored -> null)
        .add("rngcollation", INTEGER, ignored -> null)
        .add("rngsubopc", INTEGER, ignored -> null)
        .add("rngcanonical", REGPROC, ignored -> null)
        .add("rngsubdiff", REGPROC, ignored -> null)
        .build();
}
