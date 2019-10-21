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

import io.crate.analyze.user.Privilege;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.session.NamedSessionSetting;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.protocols.postgres.types.PGTypes;
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
    public PgCatalogTableDefinitions(InformationSchemaIterables informationSchemaIterables) {
        tableDefinitions = new HashMap<>(8);

        tableDefinitions.put(PgTypeTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(PGTypes.pgTypes()),
            PgTypeTable.expressions(),
            false));
        tableDefinitions.put(PgClassTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::relations,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, t.ident().fqn())
                         // we also need to check for views which have privileges set
                         || user.hasAnyPrivilege(Privilege.Clazz.VIEW, t.ident().fqn()),
            PgClassTable.expressions()
        ));
        tableDefinitions.put(PgDatabaseTable.NAME, new StaticTableDefinition<>(
            () -> completedFuture(singletonList(null)),
            PgDatabaseTable.expressions(),
            false));
        tableDefinitions.put(PgNamespaceTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::schemas,
            (user, s) -> user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, s.name()),
            PgNamespaceTable.expressions()
        ));
        tableDefinitions.put(PgAttrDefTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(Collections.emptyList()),
            PgAttrDefTable.expressions(),
            false));
        tableDefinitions.put(PgAttributeTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::columns,
            (user, c) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, c.tableInfo.ident().fqn())
                         || user.hasAnyPrivilege(Privilege.Clazz.VIEW, c.tableInfo.ident().fqn()),
            PgAttributeTable.expressions()
        ));
        tableDefinitions.put(PgIndexTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(Collections.emptyList()),
            PgIndexTable.expressions(),
            false));
        tableDefinitions.put(PgConstraintTable.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::constraints,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, t.relationName().fqn()),
            PgConstraintTable.expressions()
        ));
        tableDefinitions.put(PgDescriptionTable.NAME, new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            PgDescriptionTable.expressions(),
            false)
        );

        Iterable<NamedSessionSetting> sessionSettings =
            () -> SessionSettingRegistry.SETTINGS.entrySet().stream()
                .map(s -> new NamedSessionSetting(s.getKey(), s.getValue()))
                .iterator();
        tableDefinitions.put(PgSettingsTable.IDENT, new StaticTableDefinition<>(
            () -> sessionSettings,
            PgSettingsTable.expressions(),
            (txnCtx, settingInfo) -> settingInfo.resolveValue(txnCtx)
            )
        );
    }

    public StaticTableDefinition<?> get(RelationName relationName) {
        return tableDefinitions.get(relationName);
    }
}
