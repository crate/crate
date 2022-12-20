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

package io.crate.replication.logical.metadata.pgcatalog;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.replication.logical.metadata.Publication;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;

public class PgPublicationTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_publication");

    public static SystemTable<PublicationRow> create() {
        return SystemTable.<PublicationRow>builder(IDENT)
            .add("oid", INTEGER, p -> OidHash.publicationOid(p.name, p.publication))
            .add("pubname", STRING, p -> p.name)
            .add("pubowner", INTEGER, p -> OidHash.userOid(p.publication.owner()))
            .add("puballtables", BOOLEAN, p -> p.publication.isForAllTables())
            .add("pubinsert", BOOLEAN, p -> true)
            .add("pubupdate", BOOLEAN, p -> true)
            .add("pubdelete", BOOLEAN, p -> true)
            .build();
    }

    public static class PublicationRow {
        private final String name;
        private final Publication publication;

        public PublicationRow(String name, Publication publication) {
            this.name = name;
            this.publication = publication;
        }

        public String owner() {
            return publication.owner();
        }
    }
}
