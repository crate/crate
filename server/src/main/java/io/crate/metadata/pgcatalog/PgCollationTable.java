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

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public final class PgCollationTable {

    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_collation");

    public static SystemTable<Void> create() {
        return SystemTable.<Void>builder(NAME)
            .add("oid", INTEGER, c -> null)
            .add("collname", STRING, c -> null)
            .add("collnamespace", INTEGER, c -> null)
            .add("collowner", INTEGER , c -> null)
            .add("collprovider", STRING , c -> null)
            .add("collisdeterministic", BOOLEAN , c -> null)
            .add("collencoding", INTEGER , c -> null)
            .add("collcollate", STRING, c -> null)
            .add("collctype", STRING, c -> null)
            .add("collversion", STRING, c -> null)
            .add("xmin", INTEGER, c -> null)
            .build();
    }
}
