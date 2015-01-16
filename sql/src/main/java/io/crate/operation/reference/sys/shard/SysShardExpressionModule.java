/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.reference.sys.shard;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.shard.ShardReferenceImplementation;
import io.crate.metadata.sys.SysShardsTableInfo;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Map;

public class SysShardExpressionModule extends AbstractModule {

    static Map<ColumnIdent, Class> SHARD_COLUMNS = ImmutableMap.<ColumnIdent, Class>builder()
            .put(new ColumnIdent(ShardIdExpression.NAME), ShardIdExpression.class)
            .put(new ColumnIdent(ShardSizeExpression.NAME), ShardSizeExpression.class)
            .put(new ColumnIdent(ShardNumDocsExpression.NAME), ShardNumDocsExpression.class)
            .put(new ColumnIdent(ShardPrimaryExpression.NAME), ShardPrimaryExpression.class)
            .put(new ColumnIdent(ShardStateExpression.NAME), ShardStateExpression.class)
            .put(new ColumnIdent(ShardRelocatingNodeExpression.NAME), ShardRelocatingNodeExpression.class)
            .put(new ColumnIdent(ShardTableNameExpression.NAME), ShardTableNameExpression.class)
            .put(new ColumnIdent(ShardSchemaNameExpression.NAME), ShardSchemaNameExpression.class)
            .put(new ColumnIdent(ShardPartitionIdentExpression.NAME), ShardPartitionIdentExpression.class)
            .put(new ColumnIdent(ShardPartitionOrphanedExpression.NAME), ShardPartitionOrphanedExpression.class)
            .build();


    @Override
    protected void configure() {
        MapBinder<ReferenceIdent, ShardReferenceImplementation> b = MapBinder
                .newMapBinder(binder(), ReferenceIdent.class, ShardReferenceImplementation.class);

        Map<ColumnIdent, ReferenceInfo> infos = SysShardsTableInfo.INFOS;

        for (Map.Entry<ColumnIdent, Class> entry : SHARD_COLUMNS.entrySet()) {
            b.addBinding(infos.get(entry.getKey()).ident()).to(entry.getValue()).asEagerSingleton();
        }

    }

}
