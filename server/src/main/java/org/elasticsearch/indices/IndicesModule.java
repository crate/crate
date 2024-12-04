/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.seqno.RetentionLeaseBackgroundSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.store.IndicesStore;

import io.crate.replication.logical.LogicalReplicationSettings;
import io.crate.replication.logical.engine.SubscriberEngine;

/**
 * Configures classes and services that are shared by indices on each node.
 */
public class IndicesModule extends AbstractModule {

    public IndicesModule() {
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of();
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return Collections.emptyList();
    }

    @Override
    protected void configure() {
        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(SyncedFlushService.class).asEagerSingleton();
        bind(TransportResyncReplicationAction.class).asEagerSingleton();
        bind(PrimaryReplicaSyncer.class).asEagerSingleton();
        bind(RetentionLeaseSyncAction.class).asEagerSingleton();
        bind(RetentionLeaseBackgroundSyncAction.class).asEagerSingleton();
        bind(RetentionLeaseSyncer.class).asEagerSingleton();
    }

    public Collection<Function<IndexSettings, Optional<EngineFactory>>> getEngineFactories() {
        return List.of(
            indexSettings -> {
                if (indexSettings.getSettings().get(LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME.getKey()) != null) {
                    return Optional.of(SubscriberEngine::new);
                }
                return Optional.empty();
            }
        );
    }

}
