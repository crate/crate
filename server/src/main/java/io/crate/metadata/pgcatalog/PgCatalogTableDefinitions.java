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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.inject.Inject;

import io.crate.action.sql.Sessions;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.metadata.pgcatalog.PgPublicationTable;
import io.crate.replication.logical.metadata.pgcatalog.PgPublicationTablesTable;
import io.crate.replication.logical.metadata.pgcatalog.PgSubscriptionRelTable;
import io.crate.replication.logical.metadata.pgcatalog.PgSubscriptionTable;
import io.crate.statistics.TableStats;
import io.crate.user.Privilege;
import io.crate.user.UserManagerService;

public final class PgCatalogTableDefinitions {

    private final Map<RelationName, StaticTableDefinition<?>> tableDefinitions;

    @Inject
    public PgCatalogTableDefinitions(InformationSchemaIterables informationSchemaIterables,
                                     Sessions sessions,
                                     TableStats tableStats,
                                     PgCatalogSchemaInfo pgCatalogSchemaInfo,
                                     SessionSettingRegistry sessionSettingRegistry,
                                     Schemas schemas,
                                     LogicalReplicationService logicalReplicationService,
                                     UserManagerService userManagerService) {
        tableDefinitions = new HashMap<>();
        tableDefinitions.put(PgStatsTable.NAME, new StaticTableDefinition<>(
                tableStats::statsEntries,
                (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, t.relation().fqn()),
                PgStatsTable.create().expressions()
            )
        );
        tableDefinitions.put(PgTypeTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(PGTypes.pgTypes()),
            PgTypeTable.create().expressions(),
            false));
        tableDefinitions.put(PgClassTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::pgClasses,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, t.ident.fqn())
                         // we also need to check for views which have privileges set
                         || user.hasAnyPrivilege(Privilege.Clazz.VIEW, t.ident.fqn())
                         || isPgCatalogOrInformationSchema(t.ident.schema()),
            pgCatalogSchemaInfo.pgClassTable().expressions()
        ));
        tableDefinitions.put(PgProcTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::pgProc,
            (user, f) -> user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, f.functionName.schema())
                         || f.functionName.isBuiltin(),
            PgProcTable.create().expressions())
        );
        tableDefinitions.put(PgDatabaseTable.NAME, new StaticTableDefinition<>(
            () -> completedFuture(singletonList(null)),
            PgDatabaseTable.create().expressions(),
            false));
        tableDefinitions.put(PgNamespaceTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::schemas,
            (user, s) -> {
                if (user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, s.name()) || isPgCatalogOrInformationSchema(s.name())) {
                    return true;
                }
                for (var table : s.getTables()) {
                    if (user.hasAnyPrivilege(Privilege.Clazz.TABLE, table.ident().fqn())) {
                        return true;
                    }
                }
                for (var view : s.getViews()) {
                    if (user.hasAnyPrivilege(Privilege.Clazz.VIEW, view.ident().fqn())) {
                        return true;
                    }
                }
                return false;
            },
            PgNamespaceTable.create().expressions()
        ));
        tableDefinitions.put(PgAttrDefTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(Collections.emptyList()),
            PgAttrDefTable.create().expressions(),
            false));
        tableDefinitions.put(PgAttributeTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::columns,
            (user, c) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, c.relation().ident().fqn())
                         || user.hasAnyPrivilege(Privilege.Clazz.VIEW, c.relation().ident().fqn())
                         || isPgCatalogOrInformationSchema(c.relation().ident().schema()),
            PgAttributeTable.create().expressions()
        ));
        tableDefinitions.put(PgIndexTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.pgIndices()),
            PgIndexTable.create().expressions(),
            false));
        tableDefinitions.put(PgConstraintTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::pgConstraints,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, t.relationName().fqn())
                         || isPgCatalogOrInformationSchema(t.relationName().schema()),
            PgConstraintTable.create().expressions()
        ));
        tableDefinitions.put(PgDescriptionTable.NAME, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgDescriptionTable.create().expressions(),
            false)
        );
        tableDefinitions.put(PgRangeTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgRangeTable.create().expressions(),
            false
        ));
        tableDefinitions.put(PgEnumTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgEnumTable.create().expressions(),
            false
        ));
        tableDefinitions.put(PgRolesTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(userManagerService.roles()),
            PgRolesTable.create().expressions(),
            false
        ));
        tableDefinitions.put(PgAmTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgAmTable.create().expressions(),
            false
        ));
        tableDefinitions.put(PgTablespaceTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgTablespaceTable.create().expressions(),
            false
        ));
        tableDefinitions.put(PgSettingsTable.IDENT, new StaticTableDefinition<>(
            (txnCtx, user) -> completedFuture(sessionSettingRegistry.namedSessionSettings(txnCtx)),
            PgSettingsTable.create().expressions(),
            false
        ));
        tableDefinitions.put(PgIndexesTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgIndexesTable.create().expressions(),
            false
        ));
        tableDefinitions.put(PgLocksTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgLocksTable.create().expressions(),
            false
        ));
        Iterable<PgPublicationTable.PublicationRow> publicationRows =
            () -> logicalReplicationService.publications().entrySet().stream()
                .map(e -> new PgPublicationTable.PublicationRow(e.getKey(), e.getValue()))
                .iterator();
        tableDefinitions.put(PgPublicationTable.IDENT, new StaticTableDefinition<>(
            () -> publicationRows,
            (user, p) -> p.owner().equals(user.name()),
            PgPublicationTable.create().expressions()
        ));

        tableDefinitions.put(PgPublicationTablesTable.IDENT, new StaticTableDefinition<>(
            () -> PgPublicationTablesTable.rows(logicalReplicationService, schemas),
            (user, p) -> p.owner().equals(user.name()),
            PgPublicationTablesTable.create().expressions()
        ));

        // pg_subscription
        Iterable<PgSubscriptionTable.SubscriptionRow> subscriptionRows =
            () -> logicalReplicationService.subscriptions().entrySet().stream()
                .map(e -> new PgSubscriptionTable.SubscriptionRow(e.getKey(), e.getValue()))
                .iterator();
        tableDefinitions.put(PgSubscriptionTable.IDENT, new StaticTableDefinition<>(
            () -> subscriptionRows,
            (user, s) -> s.subscription().owner().equals(user.name()),
            PgSubscriptionTable.create().expressions()
        ));

        // pg_subscription_rel
        tableDefinitions.put(PgSubscriptionRelTable.IDENT, new StaticTableDefinition<>(
            () -> PgSubscriptionRelTable.rows(logicalReplicationService),
            (user, p) -> p.owner().equals(user.name()),
            PgSubscriptionRelTable.create().expressions()
        ));

        tableDefinitions.put(PgTablesTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::tables,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, t.ident().fqn()),
            PgTablesTable.create().expressions()
        ));

        tableDefinitions.put(PgViewsTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::views,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.VIEW, t.ident().fqn()),
            PgViewsTable.create().expressions()
        ));

        tableDefinitions.put(PgShdescriptionTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgShdescriptionTable.create().expressions(),
            false)
        );

        tableDefinitions.put(PgCursors.IDENT, new StaticTableDefinition<>(
            (txnCtx, user) -> completedFuture(sessions.getCursors(user)),
            PgCursors.create().expressions(),
            false
        ));
        tableDefinitions.put(PgEventTrigger.NAME, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgEventTrigger.create().expressions(),
            false
        ));
        tableDefinitions.put(PgDepend.NAME, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgDepend.create().expressions(),
            false
        ));
    }

    public StaticTableDefinition<?> get(RelationName relationName) {
        return tableDefinitions.get(relationName);
    }

    public static boolean isPgCatalogOrInformationSchema(String schemaName) {
        return InformationSchemaInfo.NAME.equals(schemaName) || PgCatalogSchemaInfo.NAME.equals(schemaName);
    }

    public static boolean isPgCatalogOrInformationSchema(Integer schemaOid) {
        return OidHash.schemaOid(InformationSchemaInfo.NAME) == schemaOid || OidHash.schemaOid(PgCatalogSchemaInfo.NAME) == schemaOid;
    }
}
