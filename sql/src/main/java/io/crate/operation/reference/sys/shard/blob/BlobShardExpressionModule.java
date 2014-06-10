/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.shard.blob;

import io.crate.blob.v2.BlobIndices;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.shard.blob.BlobShardReferenceImplementation;
import io.crate.metadata.sys.SysShardsTableInfo;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

public class BlobShardExpressionModule extends AbstractModule {

    private final Settings settings;

    @Inject
    public BlobShardExpressionModule(@IndexSettings Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        MapBinder<ReferenceIdent, BlobShardReferenceImplementation> binder = MapBinder
                .newMapBinder(binder(), ReferenceIdent.class, BlobShardReferenceImplementation.class);
        if (settings. getAsBoolean(BlobIndices.SETTING_BLOBS_ENABLED, false)){

            Map<ColumnIdent, ReferenceInfo> infos = SysShardsTableInfo.INFOS;
            binder.addBinding(infos.get(new ColumnIdent(BlobShardIdExpression.NAME)).ident()).to(BlobShardIdExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardNumDocsExpression.NAME)).ident()).to(BlobShardNumDocsExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardPrimaryExpression.NAME)).ident()).to(BlobShardPrimaryExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardRelocatingNodeExpression.NAME)).ident()).to(BlobShardRelocatingNodeExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardSchemaNameExpression.NAME)).ident()).to(BlobShardSchemaNameExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardSizeExpression.NAME)).ident()).to(BlobShardSizeExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardStateExpression.NAME)).ident()).to(BlobShardStateExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardTableNameExpression.NAME)).ident()).to(BlobShardTableNameExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardPartitionIdentExpression.NAME)).ident()).to(BlobShardPartitionIdentExpression.class).asEagerSingleton();
            binder.addBinding(infos.get(new ColumnIdent(BlobShardPartitionOrphanedExpression.NAME)).ident()).to(BlobShardPartitionOrphanedExpression.class).asEagerSingleton();

        }
    }
}
