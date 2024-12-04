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
import static io.crate.types.DataTypes.INTEGER_ARRAY;
import static io.crate.types.DataTypes.REGCLASS;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.SHORT_ARRAY;
import static io.crate.types.DataTypes.STRING;

import java.util.List;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.types.Regclass;

public final class PgIndexTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_index");

    private PgIndexTable() {}

    public static SystemTable<Entry> INSTANCE = SystemTable.<Entry>builder(IDENT)
        .add("indexrelid", REGCLASS, x -> x.indexRelId)
        .add("indrelid", REGCLASS, x -> x.indRelId)
        .add("indnatts", SHORT, x -> (short) 0)
        .add("indnkeyatts", SHORT, x -> (short) 0)
        .add("indisunique", BOOLEAN, x -> false)
        .add("indisprimary", BOOLEAN, x -> true)
        .add("indisexclusion", BOOLEAN, x -> false)
        .add("indimmediate", BOOLEAN, x -> true)
        .add("indisclustered", BOOLEAN, x -> false)
        .add("indisvalid", BOOLEAN, x -> true)
        .add("indcheckxmin", BOOLEAN, x -> false)
        .add("indisready", BOOLEAN, x -> true)
        .add("indislive", BOOLEAN, x -> true)
        .add("indisreplident", BOOLEAN, x -> false)
        .add("indkey", INTEGER_ARRAY, x -> x.indKey)
        .add("indcollation", INTEGER_ARRAY, x -> null)
        .add("indclass", INTEGER_ARRAY, x -> null)
        .add("indoption", SHORT_ARRAY, x -> null)
        .add("indexprs", STRING, x -> null)
        .add("indpred", STRING, x -> null)
        .build();

    public static final class Entry {

        final Regclass indRelId;
        final Regclass indexRelId;
        final List<Integer> indKey;

        public Entry(Regclass indRelId, Regclass indexRelId, List<Integer> indKey) {
            this.indRelId = indRelId;
            this.indexRelId = indexRelId;
            this.indKey = indKey;
        }
    }
}
