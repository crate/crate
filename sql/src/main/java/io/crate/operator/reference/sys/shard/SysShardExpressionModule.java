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

package io.crate.operator.reference.sys.shard;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.shard.ShardReferenceImplementation;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operator.reference.sys.shard.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Map;

public class SysShardExpressionModule extends AbstractModule {
    @Override
    protected void configure() {
        MapBinder<ReferenceIdent, ShardReferenceImplementation> b = MapBinder
                .newMapBinder(binder(), ReferenceIdent.class, ShardReferenceImplementation.class);

        Map<ColumnIdent, ReferenceInfo> infos = SysShardsTableInfo.INFOS;
        b.addBinding(infos.get(new ColumnIdent(ShardIdExpression.NAME)).ident()).to(ShardIdExpression.class).asEagerSingleton();
        b.addBinding(infos.get(new ColumnIdent(ShardSizeExpression.NAME)).ident()).to(ShardSizeExpression.class).asEagerSingleton();
        b.addBinding(infos.get(new ColumnIdent(ShardNumDocsExpression.NAME)).ident()).to(ShardNumDocsExpression.class).asEagerSingleton();
        b.addBinding(infos.get(new ColumnIdent(ShardPrimaryExpression.NAME)).ident()).to(ShardPrimaryExpression.class).asEagerSingleton();
        b.addBinding(infos.get(new ColumnIdent(ShardStateExpression.NAME)).ident()).to(ShardStateExpression.class).asEagerSingleton();
        b.addBinding(infos.get(new ColumnIdent(ShardRelocatingNodeExpression.NAME)).ident()).to(ShardRelocatingNodeExpression.class).asEagerSingleton();
        b.addBinding(infos.get(new ColumnIdent(ShardTableNameExpression.NAME)).ident()).to(ShardTableNameExpression.class).asEagerSingleton();
    }

}
