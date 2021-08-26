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

import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.SystemTable;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.replication.logical.LogicalReplicationService;

import java.util.stream.Stream;

import static io.crate.types.DataTypes.STRING;

public class PgPublicationTablesTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_publication_tables");

    public static SystemTable<PublicatedTableRow> create() {
        return SystemTable.<PublicatedTableRow>builder(IDENT)
            .add("pubname", STRING, p -> p.publicationName)
            .add("schemaname", STRING, p -> p.relationName.schema())
            .add("tablename", STRING, p -> p.relationName.name())
            .build();
    }

    public static Iterable<PublicatedTableRow> rows(LogicalReplicationService logicalReplicationService,
                                                    Schemas schemas) {
        Stream<PublicatedTableRow> s = logicalReplicationService.publications().entrySet().stream()
            .mapMulti(
                (e, c) -> {
                    var tables = e.getValue().tables();
                    if (tables.isEmpty()) {
                        // -> FOR ALL TABLES (expands to all current tables here)
                        InformationSchemaIterables.tablesStream(schemas)
                            .filter(t -> t instanceof DocTableInfo)
                            .forEach(t -> c.accept(new PublicatedTableRow(e.getKey(), t.ident(), e.getValue().owner())));
                    } else {
                        tables.forEach(t -> c.accept(new PublicatedTableRow(e.getKey(), t, e.getValue().owner())));
                    }
                }
            );
        return s::iterator;
    }

    public static class PublicatedTableRow {
        private final String publicationName;
        private final RelationName relationName;
        private final String owner;

        public PublicatedTableRow(String publicationName,
                                  RelationName relationName,
                                  String owner) {
            this.publicationName = publicationName;
            this.relationName = relationName;
            this.owner = owner;
        }

        public String owner() {
            return owner;
        }
    }
}
