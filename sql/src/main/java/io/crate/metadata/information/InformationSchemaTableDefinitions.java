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

import io.crate.metadata.TableIdent;
import io.crate.operation.collect.sources.InformationSchemaIterables;
import io.crate.operation.reference.StaticTableDefinition;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.CompletableFuture.completedFuture;

@Singleton
public class InformationSchemaTableDefinitions {

    private final Map<TableIdent, StaticTableDefinition<?>> tableDefinitions = new HashMap<>();

    @Inject
    public InformationSchemaTableDefinitions(InformationSchemaIterables informationSchemaIterables) {
        tableDefinitions.put(InformationSchemataTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.schemas()),
            InformationSchemataTableInfo.expressions()
        ));
        tableDefinitions.put(InformationTablesTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.tables()),
            InformationTablesTableInfo.expressions()
        ));
        tableDefinitions.put(InformationPartitionsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.partitions()),
            InformationPartitionsTableInfo.expressions()
        ));
        tableDefinitions.put(InformationColumnsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.columns()),
            InformationColumnsTableInfo.expression()
        ));
        tableDefinitions.put(InformationTableConstraintsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.constraints()),
            InformationTableConstraintsTableInfo.expressions()
        ));
        tableDefinitions.put(InformationRoutinesTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.routines()),
            InformationRoutinesTableInfo.expressions()
        ));
        tableDefinitions.put(InformationSqlFeaturesTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(informationSchemaIterables.features()),
            InformationSqlFeaturesTableInfo.expressions()
        ));
    }

    public StaticTableDefinition<?> get(TableIdent tableIdent) {
        return tableDefinitions.get(tableIdent);
    }
}
