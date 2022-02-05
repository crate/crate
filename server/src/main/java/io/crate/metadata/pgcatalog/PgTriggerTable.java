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
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.SHORT_ARRAY;
import static io.crate.types.DataTypes.STRING;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public final class PgTriggerTable {

    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_trigger");

    public static SystemTable<Void> create() {
        return SystemTable.<Void>builder(NAME)
            .add("oid", INTEGER, c -> null)
            .add("tgrelid", INTEGER, c -> null)
            .add("tgparentid", INTEGER, c -> null)
            .add("tgname", STRING , c -> null)
            .add("tgfoid", BOOLEAN , c -> null)
            .add("tgtype", SHORT , c -> null)
            .add("tgenabled", STRING , c -> null)
            .add("tgisinternal", BOOLEAN, c -> null)
            .add("tgconstrrelid", INTEGER, c -> null)
            .add("tgconstrindid", INTEGER, c -> null)
            .add("tgconstraint", INTEGER, c -> null)
            .add("tgdeferrable", BOOLEAN, c -> null)
            .add("tginitdeferred", BOOLEAN, c -> null)
            .add("tgnargs", SHORT, c -> null)
            // int2vector is not supported by CrateDB
            .add("tgattr", SHORT_ARRAY, c -> null)
            // bytea is not supported by CrateDB
            .add("tgargs", STRING, c -> null)
            // pg_node_tree is not supported by CrateDB
            .add("tgqual", STRING, c -> null)
            .add("tgoldtable", STRING, c -> null)
            .add("tgnewtable", STRING, c -> null)
            .build();
    }
}
