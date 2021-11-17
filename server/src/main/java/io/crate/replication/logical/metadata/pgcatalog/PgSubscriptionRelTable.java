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
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.types.Regclass;

import java.util.stream.Stream;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.REGCLASS;
import static io.crate.types.DataTypes.STRING;

public class PgSubscriptionRelTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_subscription_rel");

    public static SystemTable<PgSubscriptionRelTable.PgSubscriptionRelRow> create() {
        return SystemTable.<PgSubscriptionRelTable.PgSubscriptionRelRow>builder(IDENT)
            .add("srsubid", INTEGER, r -> r.subOid())
            .add("srrelid", REGCLASS, r -> r.relOid())
            .add("srsubstate", STRING, ignored -> null)
            .build();
    }

    public static Iterable<PgSubscriptionRelTable.PgSubscriptionRelRow> rows(LogicalReplicationService logicalReplicationService,
                                                                             Schemas schemas) {
        return () -> {
            Stream<PgSubscriptionRelTable.PgSubscriptionRelRow> s = logicalReplicationService.subscriptions().entrySet().stream()
                .mapMulti(
                    (e, c) -> {
                        var sub = e.getValue();
                        InformationSchemaIterables.tablesStream(schemas)
                            .filter(t -> {
                                if (t instanceof DocTableInfo dt) {
                                    return e.getKey().equals(REPLICATION_SUBSCRIPTION_NAME.get(dt.parameters()));
                                }
                                return false;
                            })
                            .forEach(t -> c.accept(
                                new PgSubscriptionRelRow(
                                    OidHash.subscriptionOid(e.getKey(), sub),
                                    Regclass.fromRelationName(t.ident().fqn()),
                                    sub.owner()
                                )
                            ));
                    }
                );
            return s.iterator();
        };
    }

    public record PgSubscriptionRelRow(int subOid, Regclass relOid, String owner) {}
}
