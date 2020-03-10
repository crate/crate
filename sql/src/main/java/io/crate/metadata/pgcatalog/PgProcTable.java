/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.pgcatalog;

import io.crate.metadata.FunctionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.FLOAT;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.INTEGER_ARRAY;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

public class PgProcTable {

    public static final RelationName IDENT = new RelationName(
        PgCatalogSchemaInfo.NAME,
        "pg_proc");

    public static SystemTable<Entry> create() {
        return SystemTable.<Entry>builder()
            .add("oid", INTEGER, x -> x.oid)
            .add("proname", STRING, x -> x.functionName.name())
            .add("pronamespace", INTEGER, x -> x.schemaOid)
            .add("proowner", INTEGER, x -> null)
            .add("prolang", INTEGER, x -> null)
            .add("procost", FLOAT, x -> null)
            .add("prorows", FLOAT, x -> null)
            .add("provariadic", INTEGER, x -> null)
            .add("protransform", STRING, x -> null)
            .add("proisagg", BOOLEAN, x -> null)
            .add("proiswindow", BOOLEAN, x -> null)
            .add("prosecdef", BOOLEAN, x -> null)
            .add("proleakproof", BOOLEAN, x -> null)
            .add("proisstrict", BOOLEAN, x -> null)
            .add("proretset", BOOLEAN, x -> false)
            .add("provolatile", STRING, x -> null)
            .add("proparallel", STRING, x -> null)
            .add("pronargs", SHORT, x -> null)
            .add("pronargdefaults", SHORT, x -> null)
            .add("prorettype", INTEGER, x -> null)
            .add("proargtypes", INTEGER_ARRAY, x -> null)
            .add("proallargtypes", INTEGER_ARRAY, x -> null)
            .add("proargmodes", STRING_ARRAY, x -> null)
            .add("proargnames", STRING_ARRAY, x -> null)
            .startObjectArray("proargdefaults", x -> null).endObjectArray()
            .add("protrftypes", INTEGER_ARRAY, x -> null)
            .add("prosrc", STRING_ARRAY, x -> null)
            .add("probin", STRING, x -> null)
            .add("proconfig", STRING_ARRAY, x -> null)
            .add("proacl", STRING_ARRAY, x -> null)
            .build(IDENT);
    }

    public static final class Entry {

        final int oid;
        final FunctionName functionName;
        final int schemaOid;

        public Entry(int oid, FunctionName functionName, int schemaOid) {
            this.oid = oid;
            this.functionName = functionName;
            this.schemaOid = schemaOid;
        }
    }
}
