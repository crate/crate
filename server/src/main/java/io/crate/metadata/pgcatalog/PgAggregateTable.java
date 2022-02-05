
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
import static io.crate.types.DataTypes.REGPROC;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public class PgAggregateTable {

    public static final RelationName NAME = new RelationName(
        PgCatalogSchemaInfo.NAME,
        "pg_aggregate");

    public static SystemTable<Void> create() {
        return SystemTable.<Void>builder(NAME)
            .add("aggfnoid", REGPROC, x -> null)
            .add("aggkind", STRING, x -> null)
            .add("aggnumdirectargs", SHORT, x -> null)
            .add("aggtransfn", REGPROC, x -> null)
            .add("aggfinalfn", REGPROC, x -> null)
            .add("aggcombinefn", REGPROC, x -> null)
            .add("aggserialfn", REGPROC, x -> null)
            .add("aggdeserialfn", REGPROC, x -> null)
            .add("aggmtransfn", REGPROC, x -> null)
            .add("aggminvtransfn", REGPROC, x -> null)
            .add("aggmfinalfn", REGPROC, x -> null)
            .add("aggfinalextra", BOOLEAN, x -> null)
            .add("aggmfinalextra", BOOLEAN, x -> null)
            .add("aggfinalmodify", STRING, x -> null)
            .add("aggmfinalmodify", STRING, x -> null)
            .add("aggsortop", INTEGER, x -> null)
            .add("aggtranstype", INTEGER, x -> null)
            .add("aggtransspace", INTEGER, x -> null)
            .add("aggmtranstype", INTEGER, x -> null)
            .add("aggmtransspace", INTEGER, x -> null)
            .add("agginitval", STRING, x -> null)
            .add("aggminitval", STRING, x -> null)
            .build();
    }
}
