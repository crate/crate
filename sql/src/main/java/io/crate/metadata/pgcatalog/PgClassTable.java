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

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.statistics.TableStats;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.FLOAT;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

public class PgClassTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_class");
    private static final String PERSISTENCE_PERMANENT = "p";

    public static SystemTable<Entry> create(TableStats tableStats) {
        return SystemTable.<Entry>builder(IDENT)
            .add("oid", INTEGER, x -> x.oid)
            .add("relname", STRING, x -> x.name)
            .add("relnamespace", INTEGER, x -> x.schemaOid)
            .add("reltype", INTEGER, x -> 0)
            .add("reloftype", INTEGER, x -> 0)
            .add("relowner", INTEGER, x -> 0)
            .add("relam", INTEGER, x -> 0)
            .add("relfilenode", INTEGER, x -> 0)
            .add("reltablespace", INTEGER, x -> 0)
            .add("relpages", INTEGER, x -> 0)
            .add("reltuples", FLOAT, x -> x.type.equals(Entry.Type.INDEX) ? 0f : (float) tableStats.numDocs(x.ident))
            .add("relallvisible", INTEGER, x -> 0)
            .add("reltoastrelid", INTEGER, x -> 0)
            .add("relhasindex", BOOLEAN, x -> false)
            .add("relisshared", BOOLEAN, x -> false)
            .add("relpersistence", STRING, x -> PERSISTENCE_PERMANENT)
            .add("relkind", STRING, x -> x.type.relKind)
            .add("relnatts", SHORT, x -> (short) x.numberOfAttributes)
            .add("relchecks", SHORT, x -> (short) 0)
            .add("relhasoids", BOOLEAN, x -> false)
            .add("relhaspkey", BOOLEAN, x -> x.hasPrimaryKey)
            .add("relhasrules", BOOLEAN, x -> false)
            .add("relhastriggers", BOOLEAN, x -> false)
            .add("relhassubclass", BOOLEAN, x -> false)
            .add("relrowsecurity", BOOLEAN, x -> false)
            .add("relforcerowsecurity", BOOLEAN, x -> false)
            .add("relispopulated", BOOLEAN, x -> true)
            .add("relreplident", STRING, x -> "p")
            .add("relispartition", BOOLEAN, x -> false)
            .add("relfrozenxid", INTEGER,x -> 0)
            .add("relminmxid", INTEGER, x -> 0)
            .startObjectArray("relacl", x -> null)
            .endObjectArray()
            .add("reloptions", STRING_ARRAY, x -> null)
            .startObjectArray("relpartbound", x -> null)
            .endObjectArray()
            .build();
    }

    public static final class Entry {

        public enum Type {
            VIEW("v"),
            RELATION("r"),
            INDEX("i");

            final String relKind;

            Type(String relKind) {
                this.relKind = relKind;
            }
        }

        final int oid;
        final boolean hasPrimaryKey;
        final int schemaOid;
        final RelationName ident;
        final Type type;
        final int numberOfAttributes;
        final String name;

        public Entry(int oid,
                     int schemaOid,
                     RelationName ident,
                     String name,
                     Type type,
                     int numberOfAttributes,
                     boolean hasPrimaryKey) {
            this.oid = oid;
            this.schemaOid = schemaOid;
            this.hasPrimaryKey = hasPrimaryKey;
            this.ident = ident;
            this.type = type;
            this.name = name;
            this.numberOfAttributes = numberOfAttributes;
        }
    }
}
