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

package io.crate.operator.reference.sys;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.shard.ShardReferenceImplementation;
import io.crate.operator.reference.sys.shard.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SysShardExpressionModule extends AbstractModule {
    @Override
    protected void configure() {
        MapBinder<ReferenceIdent, ShardReferenceImplementation> b = MapBinder
                .newMapBinder(binder(), ReferenceIdent.class, ShardReferenceImplementation.class);

        b.addBinding(ShardIdExpression.INFO_ID.ident()).to(ShardIdExpression.class).asEagerSingleton();
        b.addBinding(ShardSizeExpression.INFO_SIZE.ident()).to(ShardSizeExpression.class).asEagerSingleton();
        b.addBinding(ShardNumDocsExpression.INFO_NUM_DOCS.ident()).to(ShardNumDocsExpression.class).asEagerSingleton();
        b.addBinding(ShardPrimaryExpression.INFO_PRIMARY.ident()).to(ShardPrimaryExpression.class).asEagerSingleton();
        b.addBinding(ShardStateExpression.INFO_STATE.ident()).to(ShardStateExpression.class).asEagerSingleton();
        b.addBinding(ShardRelocatingNodeExpression.INFO_RELOCATING_NODE.ident()).to(ShardRelocatingNodeExpression.class).asEagerSingleton();
        b.addBinding(ShardTableNameExpression.INFO_TABLE_NAME.ident()).to(ShardTableNameExpression.class).asEagerSingleton();
    }

}
