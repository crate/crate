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

public class PgDepend {

    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_depend");

    private PgDepend() {}

    public static SystemTable<Void> create() {
        // https://www.postgresql.org/docs/current/catalog-pg-depend.html
        return SystemTable.<Void>builder(NAME)
            .add("classid", DataTypes.INTEGER, c -> null)
            .add("objid", DataTypes.INTEGER, c -> null)
            .add("objsubid", DataTypes.INTEGER, c -> null)
            .add("refclassid", DataTypes.INTEGER, c -> null)
            .add("refobjid", DataTypes.INTEGER, c -> null)
            .add("refobjsubid", DataTypes.INTEGER, c -> null)
            .add("deptype", DataTypes.CHARACTER, c -> null)
            .build();
    }
}
