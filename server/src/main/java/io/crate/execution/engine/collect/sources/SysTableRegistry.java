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

package io.crate.execution.engine.collect.sources;

import io.crate.expression.reference.StaticTableDefinition;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.sys.SysTableDefinitions;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Singleton
public class SysTableRegistry {

    private final SysSchemaInfo sysSchemaInfo;
    private final SysTableDefinitions tableDefinitions;

    @Inject
    public SysTableRegistry(SysSchemaInfo sysSchemaInfo,
                            SysTableDefinitions tableDefinitions) {
        this.sysSchemaInfo = sysSchemaInfo;
        this.tableDefinitions = tableDefinitions;
    }

    public <R> void registerSysTable(TableInfo tableInfo,
                                     Supplier<CompletableFuture<? extends Iterable<R>>> iterableSupplier,
                                     Map<ColumnIdent, ? extends RowCollectExpressionFactory<R>> expressionFactories,
                                     boolean involvesIO) {
        RelationName ident = tableInfo.ident();
        sysSchemaInfo.registerSysTable(tableInfo);
        tableDefinitions.registerTableDefinition(
            ident, new StaticTableDefinition<>(iterableSupplier, expressionFactories, involvesIO));
    }

}
