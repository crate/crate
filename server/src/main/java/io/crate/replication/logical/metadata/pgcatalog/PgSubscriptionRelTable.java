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

import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.REGCLASS;
import static io.crate.types.DataTypes.STRING;

import java.util.stream.Stream;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.types.Regclass;

public class PgSubscriptionRelTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_subscription_rel");

    public static SystemTable<PgSubscriptionRelTable.PgSubscriptionRelRow> INSTANCE = SystemTable.<PgSubscriptionRelTable.PgSubscriptionRelRow>builder(IDENT)
        .add("srsubid", INTEGER, PgSubscriptionRelRow::subOid)
        .add("srrelid", REGCLASS, PgSubscriptionRelRow::relOid)
        .add("srsubstate", STRING, PgSubscriptionRelRow::state)
        .add("srsubstate_reason", STRING, PgSubscriptionRelRow::state_reason)
        // CrateDB doesn't have Log Sequence Number per table, only a seqNo per shard (see SysShardsTable)
        .add("srsublsn", LONG, ignored -> null)
        .build();

    public static Iterable<PgSubscriptionRelTable.PgSubscriptionRelRow> rows(LogicalReplicationService logicalReplicationService) {
        return () -> {
            Stream<PgSubscriptionRelTable.PgSubscriptionRelRow> s = logicalReplicationService.subscriptions().entrySet().stream()
                .mapMulti(
                    (e, c) -> {
                        var sub = e.getValue();
                        sub.relations().forEach(
                            (r, rs) -> c.accept(
                                new PgSubscriptionRelRow(
                                    OidHash.subscriptionOid(e.getKey(), sub),
                                    new Regclass(OidHash.relationOid(OidHash.Type.TABLE, r), r.fqn()),
                                    sub.owner(),
                                    rs.state().pg_state(),
                                    rs.reason()
                                )
                            )
                        );
                    }
                );
            return s.iterator();
        };
    }

    public record PgSubscriptionRelRow(int subOid, Regclass relOid, String owner, String state, String state_reason) {}
}
