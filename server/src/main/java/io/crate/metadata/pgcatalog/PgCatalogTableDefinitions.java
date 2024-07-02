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
import java.util.Map;

import org.elasticsearch.common.inject.Inject;

import io.crate.action.sql.Sessions;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.metadata.NodeContext;
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
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.statistics.TableStats;

public final class PgCatalogTableDefinitions {

    private final Map<RelationName, StaticTableDefinition<?>> tableDefinitions;

    @Inject
    public PgCatalogTableDefinitions(InformationSchemaIterables informationSchemaIterables,
                                     Sessions sessions,
                                     TableStats tableStats,
                                     PgCatalogSchemaInfo pgCatalogSchemaInfo,
                                     SessionSettingRegistry sessionSettingRegistry,
                                     NodeContext nodeContext,
                                     LogicalReplicationService logicalReplicationService,
                                     Roles roles) {
        Schemas schemas = nodeContext.schemas();
        Iterable<PgSubscriptionTable.SubscriptionRow> subscriptionRows =
            () -> logicalReplicationService.subscriptions().entrySet().stream()
                .map(e -> new PgSubscriptionTable.SubscriptionRow(e.getKey(), e.getValue()))
                .iterator();
        Iterable<PgPublicationTable.PublicationRow> publicationRows =
            () -> logicalReplicationService.publications().entrySet().stream()
                .map(e -> new PgPublicationTable.PublicationRow(e.getKey(), e.getValue()))
                .iterator();

        tableDefinitions = Map.ofEntries(
            Map.entry(PgStatsTable.NAME, new StaticTableDefinition<>(
                    tableStats::statsEntries,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.relation().fqn()),
                    PgStatsTable.INSTANCE.expressions()
                )
            ),
            Map.entry(PgTypeTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(PGTypes.pgTypes()),
                PgTypeTable.INSTANCE.expressions(),
                false)),
            Map.entry(PgClassTable.IDENT, new StaticTableDefinition<>(
                informationSchemaIterables::pgClasses,
                (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.ident.fqn())
                            // we also need to check for views which have privileges set
                            || roles.hasAnyPrivilege(user, Securable.VIEW, t.ident.fqn())
                            || isPgCatalogOrInformationSchema(t.ident.schema()),
                pgCatalogSchemaInfo.pgClassTable().expressions()
            )),
            Map.entry(PgProcTable.IDENT, new StaticTableDefinition<>(
                informationSchemaIterables::pgProc,
                (user, f) -> roles.hasAnyPrivilege(user, Securable.SCHEMA, f.functionName.schema())
                            || f.functionName.isBuiltin(),
                PgProcTable.INSTANCE.expressions())
            ),
            Map.entry(PgDatabaseTable.NAME, new StaticTableDefinition<>(
                () -> completedFuture(singletonList(null)),
                PgDatabaseTable.INSTANCE.expressions(),
                false)),
            Map.entry(PgNamespaceTable.IDENT, new StaticTableDefinition<>(
                informationSchemaIterables::schemas,
                (user, s) -> {
                    if (roles.hasAnyPrivilege(user, Securable.SCHEMA, s.name()) ||
                        isPgCatalogOrInformationSchema(s.name())) {
                        return true;
                    }
                    for (var table : s.getTables()) {
                        if (roles.hasAnyPrivilege(user, Securable.TABLE, table.ident().fqn())) {
                            return true;
                        }
                    }
                    for (var view : s.getViews()) {
                        if (roles.hasAnyPrivilege(user, Securable.VIEW, view.ident().fqn())) {
                            return true;
                        }
                    }
                    return false;
                },
                PgNamespaceTable.INSTANCE.expressions()
            )),
            Map.entry(PgAttrDefTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(Collections.emptyList()),
                PgAttrDefTable.INSTANCE.expressions(),
                false)),
            Map.entry(PgAttributeTable.IDENT, new StaticTableDefinition<>(
                informationSchemaIterables::columns,
                (user, c) -> roles.hasAnyPrivilege(user, Securable.TABLE, c.relation().ident().fqn())
                            || roles.hasAnyPrivilege(user, Securable.VIEW, c.relation().ident().fqn())
                            || isPgCatalogOrInformationSchema(c.relation().ident().schema()),
                PgAttributeTable.INSTANCE.expressions()
            )),
            Map.entry(PgIndexTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(informationSchemaIterables.pgIndices()),
                PgIndexTable.INSTANCE.expressions(),
                false)),
            Map.entry(PgConstraintTable.IDENT, new StaticTableDefinition<>(
                informationSchemaIterables::pgConstraints,
                (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.relationName().fqn())
                            || isPgCatalogOrInformationSchema(t.relationName().schema()),
                PgConstraintTable.INSTANCE.expressions()
            )),
            Map.entry(PgDescriptionTable.NAME, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgDescriptionTable.INSTANCE.expressions(),
                false)
            ),
            Map.entry(PgRangeTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgRangeTable.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgEnumTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgEnumTable.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgRolesTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(roles.roles()),
                PgRolesTable.create(roles).expressions(),
                false
            )),
            Map.entry(PgAmTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgAmTable.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgTablespaceTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgTablespaceTable.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgSettingsTable.IDENT, new StaticTableDefinition<>(
                (txnCtx, user) -> completedFuture(sessionSettingRegistry.namedSessionSettings(txnCtx)),
                PgSettingsTable.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgIndexesTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgIndexesTable.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgLocksTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgLocksTable.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgPublicationTable.IDENT, new StaticTableDefinition<>(
                () -> publicationRows,
                (user, p) -> p.owner().equals(user.name()),
                PgPublicationTable.INSTANCE.expressions()
            )),

            Map.entry(PgPublicationTablesTable.IDENT, new StaticTableDefinition<>(
                () -> PgPublicationTablesTable.rows(logicalReplicationService, schemas),
                (user, p) -> p.owner().equals(user.name()),
                PgPublicationTablesTable.INSTANCE.expressions()
            )),

            // pg_subscription
            Map.entry(PgSubscriptionTable.IDENT, new StaticTableDefinition<>(
                () -> subscriptionRows,
                (user, s) -> s.subscription().owner().equals(user.name()),
                PgSubscriptionTable.INSTANCE.expressions()
            )),

            // pg_subscription_rel
            Map.entry(PgSubscriptionRelTable.IDENT, new StaticTableDefinition<>(
                () -> PgSubscriptionRelTable.rows(logicalReplicationService),
                (user, p) -> p.owner().equals(user.name()),
                PgSubscriptionRelTable.INSTANCE.expressions()
            )),

            Map.entry(PgTablesTable.IDENT, new StaticTableDefinition<>(
                informationSchemaIterables::tables,
                (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.ident().fqn()),
                PgTablesTable.INSTANCE.expressions()
            )),

            Map.entry(PgViewsTable.IDENT, new StaticTableDefinition<>(
                informationSchemaIterables::views,
                (user, t) -> roles.hasAnyPrivilege(user, Securable.VIEW, t.ident().fqn()),
                PgViewsTable.INSTANCE.expressions()
            )),

            Map.entry(PgShdescriptionTable.IDENT, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgShdescriptionTable.INSTANCE.expressions(),
                false)
            ),

            Map.entry(PgCursors.IDENT, new StaticTableDefinition<>(
                (txnCtx, user) -> completedFuture(sessions.getCursors(user)),
                PgCursors.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgEventTrigger.NAME, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgEventTrigger.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgDepend.NAME, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgDepend.INSTANCE.expressions(),
                false
            )),
            Map.entry(PgMatviews.NAME, new StaticTableDefinition<>(
                () -> completedFuture(emptyList()),
                PgMatviews.INSTANCE.expressions(),
                false
            ))
        );
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
