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

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.shard.ShardReferenceImplementation;
import io.crate.metadata.sys.SysShardsTableInfo;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SysShardExpressionModule extends AbstractModule {


    @Override
    protected void configure() {
        MapBinder<ReferenceIdent, ShardReferenceImplementation> b = MapBinder
                .newMapBinder(binder(), ReferenceIdent.class, ShardReferenceImplementation.class);

        b.addBinding(SysShardsTableInfo.ReferenceIdents.ID).to(ShardIdExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.SIZE).to(ShardSizeExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.NUM_DOCS).to(ShardNumDocsExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.PRIMARY).to(ShardPrimaryExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.STATE).to(ShardStateExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.RELOCATING_NODE).to(ShardRelocatingNodeExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.TABLE_NAME).to(ShardTableNameExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.SCHEMA_NAME).to(ShardSchemaNameExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.PARTITION_IDENT).to(ShardPartitionIdentExpression.class).asEagerSingleton();
        b.addBinding(SysShardsTableInfo.ReferenceIdents.ORPHAN_PARTITION).to(ShardPartitionOrphanedExpression.class).asEagerSingleton();
    }
}
