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

package io.crate.metadata.information;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.metadata.RelationName;
import io.crate.role.Permission;
import io.crate.role.Roles;
import io.crate.role.Securable;

@Singleton
public class InformationSchemaTableDefinitions {

    private final Map<RelationName, StaticTableDefinition<?>> tableDefinitions;

    @Inject
    public InformationSchemaTableDefinitions(Roles roles,
                                             InformationSchemaIterables informationSchemaIterables) {
        tableDefinitions = Map.ofEntries(
            Map.entry(
                InformationSchemataTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::schemas,
                    (user, s) -> roles.hasAnyPrivilege(user, Securable.SCHEMA, s.name()),
                    InformationSchemataTableInfo.create().expressions()
                )
            ),
            Map.entry(
                InformationTablesTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::relations,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.ident().fqn())
                                // we also need to check for views which have privileges set
                                || roles.hasAnyPrivilege(user, Securable.VIEW, t.ident().fqn()),
                    InformationTablesTableInfo.create().expressions()
                )
            ),
            Map.entry(
                InformationViewsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::views,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.VIEW, t.ident().fqn()),
                    InformationViewsTableInfo.create().expressions()
                )
            ),
            Map.entry(
                InformationPartitionsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::partitions,
                    (user, p) -> roles.hasAnyPrivilege(user, Securable.TABLE, p.name().relationName().fqn()),
                    InformationPartitionsTableInfo.create().expressions()
                )
            ),
            Map.entry(
                InformationColumnsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::columns,
                    (user, c) -> (roles.hasAnyPrivilege(user, Securable.TABLE, c.relation().ident().fqn())
                                // we also need to check for views which have privileges set
                                || roles.hasAnyPrivilege(user, Securable.VIEW, c.relation().ident().fqn())
                                ) && !c.ref().isDropped(),
                    InformationColumnsTableInfo.create().expressions()
                )
            ),
            Map.entry(
                InformationTableConstraintsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::constraints,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.relationName().fqn()),
                    InformationTableConstraintsTableInfo.create().expressions()
                )
            ),
            Map.entry(
                InformationRoutinesTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::routines,
                    (user, r) -> roles.hasAnyPrivilege(user, Securable.SCHEMA, r.schema()),
                    InformationRoutinesTableInfo.create().expressions()
                )
            ),
            Map.entry(
                InformationSqlFeaturesTableInfo.IDENT,
                new StaticTableDefinition<>(
                    () -> completedFuture(informationSchemaIterables.features()),
                    InformationSqlFeaturesTableInfo.create().expressions(),
                    false
                )
            ),
            Map.entry(
                InformationKeyColumnUsageTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::keyColumnUsage,
                    (user, k) -> roles.hasAnyPrivilege(user, Securable.TABLE, k.getFQN()),
                    InformationKeyColumnUsageTableInfo.create().expressions()
                )
            ),
            Map.entry(
                InformationReferentialConstraintsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    () -> completedFuture(informationSchemaIterables.referentialConstraintsInfos()),
                    InformationReferentialConstraintsTableInfo.create().expressions(),
                    false
                )
            ),
            Map.entry(
                InformationCharacterSetsTable.IDENT,
                new StaticTableDefinition<>(
                    () -> completedFuture(Arrays.asList(new Void[]{null})),
                    InformationCharacterSetsTable.create().expressions(),
                    false
                )
            ),
            Map.entry(
                ForeignServerTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::servers,
                    (user, t) -> user.isSuperUser() || t.owner().equals(user.name()),
                    ForeignServerTableInfo.create().expressions()
                )
            ),
            Map.entry(
                ForeignServerOptionsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::serverOptions,
                    (user, t) -> user.isSuperUser() || t.serverOwner().equals(user.name()),
                    ForeignServerOptionsTableInfo.create().expressions()
                )
            ),
            Map.entry(
                ForeignTableTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::foreignTables,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.name().fqn()),
                    ForeignTableTableInfo.create().expressions()
                )
            ),
            Map.entry(
                ForeignTableOptionsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::foreignTableOptions,
                    (user, t) -> roles.hasAnyPrivilege(user, Securable.TABLE, t.relationName().fqn()),
                    ForeignTableOptionsTableInfo.create().expressions()
                )
            ),
            Map.entry(
                UserMappingsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    informationSchemaIterables::userMappings,
                    (user, t) -> roles.hasPrivilege(user, Permission.AL, Securable.CLUSTER, null),
                    UserMappingsTableInfo.create().expressions()
                )
            ),
            Map.entry(
                UserMappingOptionsTableInfo.IDENT,
                new StaticTableDefinition<>(
                    (txnCtx, user) -> completedFuture(
                        () -> StreamSupport.stream(informationSchemaIterables.userMappingOptions().spliterator(), false)
                            .filter(userMappingOptions -> roles.hasPrivilege(user, Permission.AL, Securable.CLUSTER, null))
                            .map(userMappingOptions -> {
                                if ((user.name().equals(userMappingOptions.userName()) || user.isSuperUser()) == false) {
                                    return new UserMappingOptionsTableInfo.UserMappingOptions(
                                        userMappingOptions.userName(),
                                        userMappingOptions.serverName(),
                                        userMappingOptions.optionName(),
                                        null); // mask
                                }
                                return userMappingOptions;
                            })
                            .iterator()
                ),
                UserMappingOptionsTableInfo.create().expressions(),
                true
            ))
        );
    }

    public StaticTableDefinition<?> get(RelationName relationName) {
        return tableDefinitions.get(relationName);
    }
}
