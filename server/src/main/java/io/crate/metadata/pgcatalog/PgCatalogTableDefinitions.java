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

import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.settings.session.NamedSessionSetting;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.metadata.pgcatalog.PgPublicationTable;
import io.crate.replication.logical.metadata.pgcatalog.PgPublicationTablesTable;
import io.crate.statistics.TableStats;
import io.crate.user.Privilege;
import org.elasticsearch.common.inject.Inject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class PgCatalogTableDefinitions {

    private final Map<RelationName, StaticTableDefinition<?>> tableDefinitions;

    @Inject
    public PgCatalogTableDefinitions(InformationSchemaIterables informationSchemaIterables,
                                     TableStats tableStats,
                                     PgCatalogSchemaInfo pgCatalogSchemaInfo,
                                     SessionSettingRegistry sessionSettingRegistry,
                                     Schemas schemas,
                                     LogicalReplicationService logicalReplicationService) {
        tableDefinitions = new HashMap<>(17);
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
            (user, s) -> user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, s.name())
                         || isPgCatalogOrInformationSchema(s.name()),
            PgNamespaceTable.create().expressions()
        ));
        tableDefinitions.put(PgAttrDefTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(Collections.emptyList()),
            PgAttrDefTable.create().expressions(),
            false));
        tableDefinitions.put(PgAttributeTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::columns,
            (user, c) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, c.tableInfo.ident().fqn())
                         || user.hasAnyPrivilege(Privilege.Clazz.VIEW, c.tableInfo.ident().fqn())
                         || isPgCatalogOrInformationSchema(c.tableInfo.ident().schema()),
            PgAttributeTable.create().expressions()
        ));
        tableDefinitions.put(PgIndexTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.pgIndices()),
            PgIndexTable.create().expressions(),
            false));
        tableDefinitions.put(PgConstraintTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::constraints,
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
            () -> completedFuture(emptyList()),
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
        Iterable<NamedSessionSetting> sessionSettings =
            () -> sessionSettingRegistry.settings().entrySet().stream()
                .map(s -> new NamedSessionSetting(s.getKey(), s.getValue()))
                .iterator();
        tableDefinitions.put(PgSettingsTable.IDENT, new StaticTableDefinition<>(
            () -> sessionSettings,
            PgSettingsTable.create().expressions(),
            (txnCtx, settingInfo) -> settingInfo.resolveValue(txnCtx)
            )
        );
        tableDefinitions.put(PgIndexesTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgIndexesTable.create().expressions(),
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
    }

    public StaticTableDefinition<?> get(RelationName relationName) {
        return tableDefinitions.get(relationName);
    }

    public static boolean isPgCatalogOrInformationSchema(String schemaName) {
        return InformationSchemaInfo.NAME.equals(schemaName) || PgCatalogSchemaInfo.NAME.equals(schemaName);
    }
}
