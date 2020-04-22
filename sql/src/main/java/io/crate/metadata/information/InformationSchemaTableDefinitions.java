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

package io.crate.metadata.information;

import io.crate.analyze.user.Privilege;
import io.crate.metadata.RelationName;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.expression.reference.StaticTableDefinition;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.CompletableFuture.completedFuture;

@Singleton
public class InformationSchemaTableDefinitions {

    private final Map<RelationName, StaticTableDefinition<?>> tableDefinitions;

    @Inject
    public InformationSchemaTableDefinitions(InformationSchemaIterables informationSchemaIterables) {
        tableDefinitions = new HashMap<>(10);
        tableDefinitions.put(InformationSchemataTableInfo.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::schemas,
            (user, s) -> user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, s.name()),
            InformationSchemataTableInfo.expressions()
        ));
        tableDefinitions.put(InformationTablesTableInfo.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::relations,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, t.ident().fqn())
                         // we also need to check for views which have privileges set
                         || user.hasAnyPrivilege(Privilege.Clazz.VIEW, t.ident().fqn()),
            InformationTablesTableInfo.expressions()
        ));
        tableDefinitions.put(InformationViewsTableInfo.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::views,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.VIEW, t.ident().fqn()),
            InformationViewsTableInfo.expressions()
        ));
        tableDefinitions.put(InformationPartitionsTableInfo.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::partitions,
            (user, p) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, p.name().relationName().fqn()),
            InformationPartitionsTableInfo.create().expressions()
        ));
        tableDefinitions.put(InformationColumnsTableInfo.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::columns,
            (user, c) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, c.tableInfo.ident().fqn())
                         // we also need to check for views which have privileges set
                         || user.hasAnyPrivilege(Privilege.Clazz.VIEW, c.tableInfo.ident().fqn()),
            InformationColumnsTableInfo.create().expressions()
        ));
        tableDefinitions.put(InformationTableConstraintsTableInfo.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::constraints,
            (user, t) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, t.relationName().fqn()),
            InformationTableConstraintsTableInfo.expressions()
        ));
        tableDefinitions.put(InformationRoutinesTableInfo.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::routines,
            (user, r) -> user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, r.schema()),
            InformationRoutinesTableInfo.expressions()
        ));
        tableDefinitions.put(InformationSqlFeaturesTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.features()),
            InformationSqlFeaturesTableInfo.expressions(),
            false));
        tableDefinitions.put(InformationKeyColumnUsageTableInfo.IDENT, new StaticTableDefinition<>(
            informationSchemaIterables::keyColumnUsage,
            (user, k) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, k.getFQN()),
            InformationKeyColumnUsageTableInfo.create().expressions()
        ));
        tableDefinitions.put(InformationReferentialConstraintsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.referentialConstraintsInfos()),
            InformationReferentialConstraintsTableInfo.expressions(),
            false));
    }

    public StaticTableDefinition<?> get(RelationName relationName) {
        return tableDefinitions.get(relationName);
    }
}
