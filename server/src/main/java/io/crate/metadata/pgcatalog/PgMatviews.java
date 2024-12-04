/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class PgMatviews {
    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_matviews");

    private PgMatviews() {}

    // https://www.postgresql.org/docs/current/view-pg-matviews.html
    public static SystemTable<Void> INSTANCE = SystemTable.<Void>builder(NAME)
        .add("schemaname", DataTypes.STRING, c -> null)
        .add("matviewname", DataTypes.STRING, c -> null)
        .add("matviewowner", DataTypes.STRING, c -> null)
        .add("tablespace", DataTypes.STRING, c -> null)
        .add("hasindexes", DataTypes.BOOLEAN, c -> null)
        .add("ispopulated", DataTypes.BOOLEAN, c -> null)
        .add("definition", DataTypes.STRING, c -> null)
        .build();
}
